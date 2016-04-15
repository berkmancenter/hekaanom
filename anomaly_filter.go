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
	*pline
	*AnomalyConfig
	windower Windower
	detector Detector
	gatherer Gatherer
	binner   Binner
}

type pline struct {
	closed  bool
	done    chan struct{}
	errc    chan error
	metrics chan Metric
	windows chan Window
	rulings chan Ruling
	spans   chan AnomalousSpan
	bins    chan Bin
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
	f.pline = &pline{
		done:    make(chan struct{}),
		errc:    make(chan error, 7),
		metrics: make(chan Metric),
		windows: make(chan Window),
		rulings: make(chan Ruling),
		spans:   make(chan AnomalousSpan),
		bins:    make(chan Bin),
	}
	// TODO err channel
	f.windower.Connect(f.pline.metrics, f.pline.windows, f.pline.done)
	f.detector.Connect(f.pline.windows, f.pline.rulings, f.pline.done)
	f.gatherer.Connect(f.pline.rulings, f.pline.spans, f.pline.done)
	toBin, toPublish := make(chan AnomalousSpan), make(chan AnomalousSpan)
	spanBroadcast(f.pline.spans, []chan AnomalousSpan{toBin, toPublish}, f.pline.done)
	f.binner.Connect(toBin, f.pline.bins, f.pline.done)
	f.publishSpans(toPublish, f.pline.done)
	f.publishBins(f.pline.bins, f.pline.done)
	go func() {
		println("Listen for stop")
		<-fr.StopChan()
		println("Stop")
	}()
	return nil
}

func (f *AnomalyFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	metric := f.metricFromMessage(pack.Message)
	select {
	case <-f.pline.done:
		return nil
	default:
		f.pline.metrics <- metric
		f.runner.UpdateCursor(pack.QueueCursor)
	}
	return nil
}

// TODO Farm this out to the componenets
func (f *AnomalyFilter) TimerEvent() error {
	now := time.Now()
	f.gatherer.FlushExpiredSpans(now, f.pline.spans)
	return nil
}

func (f *AnomalyFilter) CleanUp() {
	println("Cleanup")
	/*
		close(f.pline.windows)
		close(f.pline.rulings)
		close(f.pline.spans)
		close(f.pline.bins)
	*/
}

func (f *AnomalyFilter) publishSpans(in chan AnomalousSpan, done <-chan struct{}) {
	go func() {
		for span := range in {
			select {
			case <-done:
				return
			default:
			}
			newPack, err := f.helper.PipelinePack(0)
			if err != nil {
				fmt.Println(err)
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
}

func (f *AnomalyFilter) publishBins(in chan Bin, done <-chan struct{}) {
	go func() {
		for bin := range in {
			select {
			case <-done:
				return
			default:
			}
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

func spanBroadcast(in chan AnomalousSpan, out []chan AnomalousSpan, done <-chan struct{}) {
	go func() {
	SpanLoop:
		for span := range in {
			for _, outChan := range out {
				select {
				case outChan <- span:
				case <-done:
					break SpanLoop
				}
			}
		}

		for _, outChan := range out {
			close(outChan)
		}
	}()
}
