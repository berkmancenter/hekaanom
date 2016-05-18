package hekaanom

import (
	"bytes"
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
			}
		})
}

type AnomalyConfig struct {
	SeriesFields []string      `toml:"series_fields"`
	ValueField   string        `toml:"value_field"`
	Realtime     bool          `toml:"realtime"`
	WindowConfig *WindowConfig `toml:"window"`
	DetectConfig *DetectConfig `toml:"detect"`
	GatherConfig *GatherConfig `toml:"gather"`
}

type AnomalyFilter struct {
	runner pipeline.FilterRunner
	helper pipeline.PluginHelper
	*AnomalyConfig
	windower Windower
	detector Detector
	gatherer Gatherer
	metrics  chan Metric
	spans    chan Span
}

func (f *AnomalyFilter) ConfigStruct() interface{} {
	return &AnomalyConfig{
		WindowConfig: f.windower.ConfigStruct().(*WindowConfig),
		DetectConfig: f.detector.ConfigStruct().(*DetectConfig),
		GatherConfig: f.gatherer.ConfigStruct().(*GatherConfig),
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

	return nil
}

func (f *AnomalyFilter) Prepare(fr pipeline.FilterRunner, h pipeline.PluginHelper) error {
	f.runner = fr
	f.helper = h
	f.metrics = make(chan Metric)
	f.spans = make(chan Span)

	windows := f.windower.Connect(f.metrics)
	rulings := f.detector.Connect(windows)
	f.spans = f.gatherer.Connect(rulings)

	f.publishSpans(f.spans)

	return nil
}

func (f *AnomalyFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	metric := f.metricFromMessage(pack.Message)
	f.metrics <- metric
	return nil
}

func (f *AnomalyFilter) TimerEvent() error {
	f.detector.PrintQs()
	//f.gatherer.PrintSpansInMem()
	now := time.Now()
	if f.AnomalyConfig.Realtime {
		f.gatherer.FlushExpiredSpans(now, f.spans)
	}
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
		f.getMessagePassthrough(msg),
	}
}

func (f *AnomalyFilter) getMessageSeries(msg *message.Message) string {
	var series bytes.Buffer
	var values []string

	for _, field := range f.AnomalyConfig.SeriesFields {
		f := msg.FindFirstField(field)
		if f == nil {
			continue
		}
		val := f.GetValueString()
		values = append(values, val...)
	}

	for i, val := range values {
		series.WriteString(val)
		if i < len(values)-1 {
			series.WriteString("|")
		}
	}

	seriesStr := series.String()
	if len(seriesStr) == 0 {
		return DefaultMessageSeries
	}
	return seriesStr
}

func (f *AnomalyFilter) getMessagePassthrough(msg *message.Message) []*message.Field {
	var fields []*message.Field
	for _, field := range f.AnomalyConfig.SeriesFields {
		f := msg.FindFirstField(field)
		if f != nil {
			fields = append(fields, f)
		}
	}
	return fields
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
