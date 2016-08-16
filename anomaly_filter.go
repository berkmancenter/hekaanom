package hekaanom

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

const timeFormat = time.RFC3339Nano

var (
	defaultMessageVal    = 1.0
	defaultMessageSeries = "**all**"
)

func init() {
	pipeline.RegisterPlugin("AnomalyFilter",
		func() interface{} {
			return &AnomalyFilter{
				windower: new(windowFilter),
				detector: new(detectFilter),
				gatherer: new(gatherFilter),
			}
		})
}

type AnomalyConfig struct {
	// A space-delimited list of fields that should be used to group metrics into
	// time series. The series code will be the values of these fields joined by
	// '||'.
	SeriesFields []string `toml:"series_fields"`

	// The name of the field in the incoming metric message that contains the
	// value that should be used to create the time series.
	ValueField string `toml:"value_field"`

	// Is this filter running against realtime data? i.e. is data going to keep
	// coming in forever?
	Realtime bool `toml:"realtime"`

	// The configuration for the filter which groups metrics together into
	// regular time blocks. The value of each window is the sum of the
	// constituent metric values.
	WindowConfig *WindowConfig `toml:"window"`

	// The configuration for the filter that detects anomalies in a time series
	// made of windows.
	DetectConfig *DetectConfig `toml:"detect"`

	// The configuration for the filter that gathers roughly consecutive
	// anomalies into anomalous spans of time, a.k.a. anomalous events.
	GatherConfig *GatherConfig `toml:"gather"`

	// Output debugging information.
	Debug bool `toml:"debug"`
}

type AnomalyFilter struct {
	runner pipeline.FilterRunner
	helper pipeline.PluginHelper
	*AnomalyConfig
	windower   windower
	detector   detector
	gatherer   gatherer
	metrics    chan metric
	spans      chan span
	processing bool
}

func (f *AnomalyFilter) ConfigStruct() interface{} {
	return &AnomalyConfig{
		WindowConfig: f.windower.ConfigStruct().(*WindowConfig),
		DetectConfig: f.detector.ConfigStruct().(*DetectConfig),
		GatherConfig: f.gatherer.ConfigStruct().(*GatherConfig),
		Debug:        false,
	}
}

func (f *AnomalyFilter) Init(config interface{}) error {
	f.AnomalyConfig = config.(*AnomalyConfig)
	f.processing = false

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
	f.metrics = make(chan metric)

	windows := f.windower.Connect(f.metrics)
	rulings := f.detector.Connect(windows)

	if f.AnomalyConfig.GatherConfig.Disabled {
		f.publishRulings(rulings)
	} else {
		rulingChans := broadcastRuling(rulings, 2)
		f.publishRulings(rulingChans[0])
		f.spans = f.gatherer.Connect(rulingChans[1])
		f.publishSpans(f.spans)
	}

	return nil
}

func (f *AnomalyFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	metric := f.metricFromMessage(pack.Message)
	f.metrics <- metric
	f.runner.UpdateCursor(pack.QueueCursor)
	if !f.processing {
		f.processing = true
		f.runner.LogMessage("Processing started.")
	}
	return nil
}

func (f *AnomalyFilter) TimerEvent() error {
	if f.AnomalyConfig.Debug {
		f.detector.PrintQs()
		f.gatherer.PrintSpansInMem()
	}

	// We should only be keeping track of the real "now" if we're doing realtime
	// analysis.
	if f.AnomalyConfig.Realtime {
		now := time.Now()
		f.gatherer.FlushExpiredSpans(now, f.spans)
	}

	if f.processing && f.detector.QueuesEmpty() {
		f.runner.LogMessage("All queues emptied.")
		f.processing = false
	}
	return nil
}

func (f *AnomalyFilter) CleanUp() {
	close(f.metrics)
}

func (f *AnomalyFilter) publishSpans(in chan span) error {
	go func() {
		for span := range in {
			newPack, err := f.helper.PipelinePack(0)
			if err != nil {
				fmt.Println("Could not create new span message")
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
	return nil
}

func (f *AnomalyFilter) publishRulings(in chan ruling) error {
	go func() {
		for ruling := range in {
			newPack, err := f.helper.PipelinePack(0)
			if err != nil {
				fmt.Println("Could not create new ruling message")
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

func (f *AnomalyFilter) metricFromMessage(msg *message.Message) metric {
	return metric{
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
		return defaultMessageSeries
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

func broadcastSpan(in chan span, numOut int) []chan span {
	out := make([]chan span, numOut)
	for i := 0; i < numOut; i++ {
		out[i] = make(chan span)
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

func broadcastRuling(in chan ruling, numOut int) []chan ruling {
	out := make([]chan ruling, numOut)
	for i := 0; i < numOut; i++ {
		out[i] = make(chan ruling)
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
		return defaultMessageVal
	}
	value, ok := msg.GetFieldValue(f.AnomalyConfig.ValueField)
	if !ok {
		return defaultMessageVal
	}
	floatVal, err := strconv.ParseFloat(value.(string), 64)
	if err != nil {
		return defaultMessageVal
	}
	return floatVal
}
