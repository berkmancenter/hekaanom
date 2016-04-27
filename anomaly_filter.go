package hekaanom

import (
	"fmt"
	"strconv"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

const TimeFormat = time.RFC3339Nano

var (
	DefaultMessageVal    = 1.0
	DefaultMessageSeries = "**all**"
)

func init() {
	pipeline.RegisterPlugin("AnomalyFilter",
		func() interface{} {
			return &AnomalyFilter{
				windower: new(WindowFilter),
				detector: new(DetectFilter),
				gatherer: new(GatherFilter),
				binner:   new(BinFilter),
			}
		})
}

type AnomalyConfig struct {
	SeriesField  string        `toml:"series_field"`
	ValueField   string        `toml:"value_field"`
	WindowConfig *WindowConfig `toml:"window"`
	DetectConfig *DetectConfig `toml:"detect"`
	GatherConfig *GatherConfig `toml:"gather"`
	BinConfig    *BinConfig    `toml:"bin"`
}

type AnomalyFilter struct {
	runner pipeline.FilterRunner
	helper pipeline.PluginHelper
	*AnomalyConfig
	windower Windower
	detector Detector
	gatherer Gatherer
	binner   Binner
	metrics  chan Metric
	spans    chan Span
}

func (f *AnomalyFilter) ConfigStruct() interface{} {
	return &AnomalyConfig{
		WindowConfig: f.windower.ConfigStruct().(*WindowConfig),
		DetectConfig: f.detector.ConfigStruct().(*DetectConfig),
		GatherConfig: f.gatherer.ConfigStruct().(*GatherConfig),
		BinConfig:    f.binner.ConfigStruct().(*BinConfig),
	}
}

func (f *AnomalyFilter) Init(config interface{}) error {
	f.AnomalyConfig = config.(*AnomalyConfig)

	if err := f.windower.Init(f.AnomalyConfig.WindowConfig); err != nil {
		return err
	}
	if err := f.detector.Init(f.AnomalyConfig.DetectConfig); err != nil {
		return err
	}
	if err := f.gatherer.Init(f.AnomalyConfig.GatherConfig); err != nil {
		return err
	}
	if err := f.binner.Init(f.AnomalyConfig.BinConfig); err != nil {
		return err
	}

	return nil
}

func (f *AnomalyFilter) Prepare(fr pipeline.FilterRunner, h pipeline.PluginHelper) error {
	f.runner = fr
	f.helper = h
	f.metrics = make(chan Metric)
	f.spans = make(chan Span)

	windows := f.windower.Connect(f.metrics)
	rulings := f.detector.Connect(windows)

	rulingChans := broadcastRuling(rulings, 2)
	f.spans = f.gatherer.Connect(rulings)

	spanChans := broadcastSpan(f.spans, 2)

	bins := f.binner.Connect(f.spans)

	f.publishRulings(rulingChans[1])
	f.publishSpans(spanChans[1])
	f.publishBins(bins)

	return nil
}

func (f *AnomalyFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	metric := f.metricFromMessage(pack.Message)
	f.metrics <- metric
	return nil
}

func (f *AnomalyFilter) TimerEvent() error {
	f.detector.PrintQs()
	now := time.Now()
	f.gatherer.FlushExpiredSpans(now, f.spans)
	return nil
}

func (f *AnomalyFilter) CleanUp() {
	close(f.metrics)
}

func (f *AnomalyFilter) publishSpans(in chan Span) error {
	go func() {
		for span := range in {
			newPack, err := f.helper.PipelinePack(0)
			if err != nil {
				fmt.Println("Could not create new span message")
				continue
			}
			msg := newPack.Message
			msg.SetType("anom.span")
			if err = span.FillMessage(msg); err != nil {
				fmt.Println(err)
				continue
			}
			f.runner.Inject(newPack)
		}
	}()
	return nil
}

func (f *AnomalyFilter) publishBins(in chan Bin) error {
	go func() {
		for bin := range in {
			newPack, err := f.helper.PipelinePack(0)
			if err != nil {
				fmt.Println(err)
				continue
			}
			msg := newPack.Message
			msg.SetType("anom.bin")
			if err = bin.FillMessage(msg); err != nil {
				fmt.Println(err)
				continue
			}
			f.runner.Inject(newPack)
		}
	}()
	return nil
}

func (f *AnomalyFilter) publishRulings(in chan Ruling) error {
	go func() {
		for ruling := range in {
			newPack, err := f.helper.PipelinePack(0)
			if err != nil {
				fmt.Println(err)
				continue
			}
			msg := newPack.Message
			msg.SetType("anom.ruling")
			if err = ruling.FillMessage(msg); err != nil {
				fmt.Println(err)
				continue
			}
			f.runner.Inject(newPack)
		}
	}()
	return nil
}

func (f *AnomalyFilter) metricFromMessage(msg *message.Message) Metric {
	return Metric{
		time.Unix(0, msg.GetTimestamp()),
		f.getMessageSeries(msg),
		f.getMessageValue(msg),
	}
}

func (f *AnomalyFilter) getMessageSeries(msg *message.Message) string {
	if f.AnomalyConfig.SeriesField == "" {
		return DefaultMessageSeries
	}
	value, ok := msg.GetFieldValue(f.AnomalyConfig.SeriesField)
	if !ok {
		return DefaultMessageSeries
	}
	return value.(string)
}

func broadcastSpan(in chan Span, numOut int) []chan Span {
	out := make([]chan Span, numOut)
	for i := 0; i < numOut; i++ {
		out[i] = make(chan Span)
	}
	go func() {
		defer func() {
			for _, ch := range out {
				close(ch)
			}
		}()
		for msg := range in {
			for _, outChan := range out {
				outChan <- msg
			}
		}
	}()
	return out
}

func broadcastRuling(in chan Ruling, numOut int) []chan Ruling {
	out := make([]chan Ruling, numOut)
	for i := 0; i < numOut; i++ {
		out[i] = make(chan Ruling)
	}
	go func() {
		defer func() {
			for _, ch := range out {
				close(ch)
			}
		}()
		for msg := range in {
			for _, ch := range out {
				ch <- msg
			}
		}
	}()
	return out
}

func (f *AnomalyFilter) getMessageValue(msg *message.Message) float64 {
	if f.AnomalyConfig.ValueField == "" {
		return DefaultMessageVal
	}
	value, ok := msg.GetFieldValue(f.AnomalyConfig.ValueField)
	if !ok {
		return DefaultMessageVal
	}
	floatVal, err := strconv.ParseFloat(value.(string), 64)
	if err != nil {
		return DefaultMessageVal
	}
	return floatVal
}
