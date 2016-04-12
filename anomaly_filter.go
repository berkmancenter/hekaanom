package hekaanom

import (
	"errors"
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
	SeriesField  string        `toml:"series_field"`
	ValueField   string        `toml:"value_field"`
	WindowConfig *WindowConfig `toml:"window"`
	DetectConfig *DetectConfig `toml:"detect"`
	GatherConfig *GatherConfig `toml:"gather"`
}

type AnomalyFilter struct {
	runner pipeline.FilterRunner
	helper pipeline.PluginHelper
	*anomPipeline
	*AnomalyConfig
	windower Windower
	detector Detector
	gatherer Gatherer
}

type anomPipeline struct {
	metrics chan Metric
	windows chan Window
	rulings chan Ruling
	spans   chan AnomalousSpan
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
	f.anomPipeline = &anomPipeline{
		metrics: make(chan Metric, 100),
		windows: make(chan Window, 100),
		rulings: make(chan Ruling, 100),
		spans:   make(chan AnomalousSpan, 100),
	}
	// TODO err channel
	go f.windower.Connect(f.anomPipeline.metrics, f.anomPipeline.windows)
	go f.detector.Connect(f.anomPipeline.windows, f.anomPipeline.rulings)
	go f.gatherer.Connect(f.anomPipeline.rulings, f.anomPipeline.spans)
	go f.publishSpans()
	return nil
}

func (f *AnomalyFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	metric := f.metricFromMessage(pack.Message)
	f.anomPipeline.metrics <- metric
	return nil
}

func (f *AnomalyFilter) TimerEvent() error {
	now := time.Now()
	f.gatherer.FlushExpiredSpans(now, f.anomPipeline.spans)
	return nil
}

func (f *AnomalyFilter) CleanUp() {
	close(f.anomPipeline.metrics)
	close(f.anomPipeline.windows)
	close(f.anomPipeline.rulings)
	close(f.anomPipeline.spans)
}

func (f *AnomalyFilter) publishSpans() error {
	for span := range f.anomPipeline.spans {
		newPack, err := f.helper.PipelinePack(0)
		if err != nil {
			return errors.New("Could not create new span message")
		}
		msg := newPack.Message
		msg.SetType("anom.span")
		if err = span.FillMessage(msg); err != nil {
			fmt.Println(err)
			return err
		}
		f.runner.Inject(newPack)
	}
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
