package hekaanom

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/mozilla-services/heka/pipeline"
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
	Connect(in chan Ruling) chan Span
	FlushExpiredSpans(now time.Time, out chan Span)
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
	spans      map[string]*Span
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
	}
	if f.GatherConfig.SampleInterval <= 0 {
		return errors.New("'sample_interval' must be greater than zero.")
	}

	f.aggregator = f.getAggregator()
	f.spans = map[string]*Span{}
	return nil
}

func (f *GatherFilter) Connect(in chan Ruling) chan Span {
	out := make(chan Span)

	go func() {
		defer close(out)

		for ruling := range in {
			f.FlushExpiredSpans(ruling.Window.End, out)
			if !ruling.Anomalous {
				continue
			}

			span, ok := f.spans[ruling.Window.Series]
			if !ok {
				span = &Span{
					Series: ruling.Window.Series,
					Values: make([]float64, 1),
					Start:  ruling.Window.End,
				}
				f.spans[ruling.Window.Series] = span
			}

			value, err := f.getRulingValue(ruling)
			if err != nil {
				fmt.Println(err)
				continue
			}
			span.Values = append(span.Values, value)
			agg, err := f.aggregator(span.Values)
			if err != nil {
				fmt.Println(err)
				continue
			}
			span.Aggregation = agg
			span.End = ruling.Window.End
		}
	}()
	return out
}

func (f *GatherFilter) FlushExpiredSpans(now time.Time, out chan Span) {
	for series, span := range f.spans {
		if int64(now.Sub(span.End)/time.Second) > f.GatherConfig.SpanWidth {
			f.flushSpan(span, out)
			delete(f.spans, series)
		}
	}
}

func (f *GatherFilter) flushSpan(span *Span, out chan Span) {
	span.Duration = span.End.Sub(span.Start)
	if span.Duration == 0.0 {
		span.Duration = time.Duration(f.GatherConfig.SampleInterval) * time.Second
	}
	span.Score = float64(span.Duration/time.Second) * span.Aggregation
	out <- *span
}

func (f *GatherFilter) getRulingValue(ruling Ruling) (float64, error) {
	st := reflect.ValueOf(ruling)
	value := reflect.Indirect(st).FieldByName(f.GatherConfig.ValueField)
	if !value.IsValid() {
		return 0.0, errors.New("Ruling did not contain field.")
	}
	return value.Float(), nil
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
