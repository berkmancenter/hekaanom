// This package implements a generic anomaly detection filter interface.
package filter

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/berkmancenter/hekaanom"
	"github.com/berkmancenter/hekaanom/detect"
	"github.com/berkmancenter/hekaanom/gather"
	"github.com/berkmancenter/hekaanom/window"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

var (
	DefaultMessageVal    = 1.0
	DefaultMessageSeries = "**all**"
)

func init() {
	pipeline.RegisterPlugin("AnomalyFilter",
		func() interface{} {
			return &AnomalyFilter{
				windower: new(window.WindowFilter),
				detector: new(detector.DetectFilter),
				gatherer: new(gather.GatherFilter),
			}
		})
}

type AnomalyConfig struct {
	SeriesField  string               `toml:"series_field"`
	ValueField   string               `toml:"value_field"`
	WindowConfig *window.WindowConfig `toml:"window"`
	DetectConfig *detect.DetectConfig `toml:"detect"`
	GatherConfig *gather.GatherConfig `toml:"gather"`
}

type AnomalyFilter struct {
	runner pipeline.FilterRunner
	helper pipeline.PluginHelper
	*anomPipeline
	*AnomalyConfig
	windower window.Windower
	detector detect.Detector
	gatherer gather.Gatherer
}

type anomPipeline struct {
	metrics chan hekaanom.Metric
	windows chan hekaanom.Window
	rulings chan hekaanom.Ruling
	spans   chan hekaanom.Span
}

func (f *AnomalyFilter) ConfigStruct() interface{} {
	return &AnomalyConfig{
		WindowConfig: f.windower.ConfigStruct(),
		DetectConfig: f.detector.ConfigStruct(),
		GatherConfig: f.gatherer.ConfigStruct(),
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
	f.anomPipeline = anomPipeline{
		metrics: make(chan hekaanom.Metric, 100),
		windows: make(chan hekaanom.Window, 100),
		rulings: make(chan hekaanom.Ruling, 100),
		spans:   make(chan hekaanom.Span, 100),
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
}

func (f *AnomalyFilter) TimerEvent() error {
	now := time.Now()
	f.gatherer.FlushExpiredSpans(now)
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
			return err
		}
		f.runner.Inject(newPack)
	}
	return nil
}

func algoIsKnown(algo string) bool {
	for _, v := range Algos {
		if v == algo {
			return true
		}
	}
	return false
}

func (f *AnomalyFilter) metricFromMessage(msg *message.Message) hekaanom.Metric {
	return hekaanom.Metric{
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
