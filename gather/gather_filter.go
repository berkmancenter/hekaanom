// This package implements a filter that groups together consecutive anomalous
// detections into single anomalous spans.
package gather

import (
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/mozilla-services/heka/pipeline"

	"github.com/berkmancenter/hekaanom"
)

var (
	DefaultAggregator = "Sum"
	DefaultValueField = "normed"
	AggFunctions      = map[string]func(stats.Float64Data) (float64, error){
		"Sum":      stats.Sum,
		"Mean":     stats.Mean,
		"Median":   stats.Median,
		"MAD":      stats.MedianAbsoluteDeviation,
		"Midhinge": stats.Midhinge,
		"Trimean":  stats.Trimean,
	}
)

type Gatherer interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in chan hekaanom.Ruling, out chan Span) error
}

type GatherConfig struct {
	SpanWidth      int64 `toml:"span_width"`
	Statistic      string
	ValueField     string `toml:"value_field"`
	SampleInterval int64  `toml:"sample_interval"`
}

type GatherFilter struct {
	*GatherConfig
	aggregator func(stats.Float64Data) (float64, error)
	spans      map[string]*hekaanom.AnomalousSpan
}

func (f *GatherFilter) ConfigStruct() interface{} {
	return &GatherConfig{
		Statistic:  DefaultAggregator,
		ValueField: DefaultValueField,
	}
}

func (f *GatherFilter) Init(config interface{}) error {
	f.GatherConfig = config.(*GatherConfig)

	if f.GatherConfig.SpanWidth <= 0 {
		return errors.New("'span_width' must be greater than zero.")
	} else {
		f.GatherConfig.SpanWidth = time.Duration(f.GatherConfig.SpanWidth) * time.Second
	}

	if f.GatherConfig.SampleInterval <= 0 {
		return errors.New("'sample_interval' must be greater than zero.")
	}

	f.aggregator = f.getAggregator()
	f.spans = map[string]*hekaanom.AnomalousSpan{}
	return nil
}

func (f *GatherFilter) Connect(in chan hekaanom.Ruling, out chan hekaanom.AnomalousSpan) error {
	for ruling := range in {
		f.FlushExpiredSpans(ruling.Window.End, out)

		span, ok := f.spans[ruling.Window.Series]
		if !ok {
			span = &hekaanom.AnomalousSpan{
				Series: ruling.Window.Series,
				Values: make([]float64, 1),
			}
			f.spans[ruling.Window.Series] = span
		}

		if span.Start.IsZero() {
			span.Start = ruling.Window.End
		}

		value, err := f.getRulingValue(ruling)
		if err != nil {
			return err
		}
		span.Values = append(span.Values, value)
		agg, err := f.aggregator(span.Values)
		if err != nil {
			return err
		}
		span.Aggregation = agg
		span.End = ruling.Window.End
	}
	return nil
}

func (f *GatherFilter) FlushExpiredSpans(now time.Time, out chan hekaanom.AnomalousSpan) {
	for series, span := range f.spans {
		if now.Sub(span.End) > f.GatherConfig.SpanWidth {
			f.flushSpan(span, out)
			delete(f.spans, series)
		}
	}
}

func (f *GatherFilter) flushSpan(span *hekaanom.AnomalousSpan, out chan hekaanom.AnomalousSpan) {
	span.Duration = span.End.Sub(span.Start)
	span.Score = math.Max(float64(span.Duration/time.Second), f.GatherConfig.SampleInterval) * span.Aggregation
	out <- span
}

func (f *GatherFilter) getRulingValue(ruling hekaanom.Ruling) (float64, error) {
	st := reflect.TypeOf(ruling)
	value := st.FieldByName(f.GatherConfig.ValueField)
	if !value.IsValid() {
		return 0.0, errors.New("Ruling did not contain field.")
	}
	return value, nil
}

func (f *GatherFilter) getAggregator() func(stats.Float64Data) (float64, error) {
	if f.GatherConfig.Statistic == "" {
		return AggFunctions[DefaultAggregator]
	}
	if f, ok := AggFunctions[f.GatherConfig.Statistic]; ok {
		return f
	}
	return AggFunctions[DefaultAggregator]
}
