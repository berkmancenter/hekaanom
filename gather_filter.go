package hekaanom

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
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
	FlushStuckSpans(out chan Span)
	PrintSpansInMem()
}

type GatherConfig struct {
	SpanWidth  int64 `toml:"span_width"`
	Statistic  string
	ValueField string `toml:"value_field"`
	LastDate   string `toml:"last_date"`
}

type GatherFilter struct {
	*GatherConfig
	aggregator func(stats.Float64Data) (float64, error)
	spanCache  spanCache
	lastDate   time.Time
}

type spanCache struct {
	sync.Mutex
	spans map[string]*Span
	nows  map[string]time.Time
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

	if f.GatherConfig.LastDate == "today" {
		f.lastDate = time.Now()
	} else if f.GatherConfig.LastDate == "yesterday" {
		f.lastDate = time.Now().Add(-1 * time.Duration(24) * time.Hour)
	} else {
		lastDate, err := time.Parse(time.RFC3339, f.GatherConfig.LastDate)
		if err != nil {
			return err
		}
		f.lastDate = lastDate
	}

	f.aggregator = f.getAggregator()
	f.spanCache = spanCache{spans: map[string]*Span{}, nows: map[string]time.Time{}}
	return nil
}

func (f *GatherFilter) Connect(in chan Ruling) chan Span {
	out := make(chan Span)

	go func() {
		defer close(out)

		for ruling := range in {
			thisSeries := ruling.Window.Series

			f.spanCache.Lock()
			f.spanCache.nows[thisSeries] = ruling.Window.End

			span, ok := f.spanCache.spans[thisSeries]
			if ok {
				now := f.spanCache.nows[span.Series]
				f.FlushIfExpired(span, now, out)
			} else {
				// If this ruling isn't anomalous, don't start keeping track of a new span.
				if !ruling.Anomalous {
					continue
				}
				span = &Span{
					Series:      thisSeries,
					Values:      []float64{},
					Start:       ruling.Window.Start,
					End:         ruling.Window.End,
					Passthrough: ruling.Window.Passthrough,
				}
				f.spanCache.spans[thisSeries] = span
			}
			f.spanCache.Unlock()

			value, err := f.getRulingValue(ruling)
			if err != nil {
				fmt.Println(err)
				continue
			}
			span.Values = append(span.Values, value)
			// If this ruling is anomlous, bump the end of this span out so we
			// continue to keep track of it.
			if ruling.Anomalous {
				span.End = ruling.Window.End
			}
		}
	}()
	return out
}

func (f *GatherFilter) FlushIfExpired(span *Span, now time.Time, out chan Span) {
	// Only called from within a goroutine that already locks spanCache for
	// writing, so we don't need to lock here.
	age := int64(now.Sub(span.End) / time.Second)
	willExpireAt := span.End.Add(time.Duration(f.GatherConfig.SpanWidth) * time.Second)

	if age >= f.GatherConfig.SpanWidth || willExpireAt.Equal(f.lastDate) || willExpireAt.After(f.lastDate) {
		f.flushSpan(span, out)
		delete(f.spanCache.spans, span.Series)
		delete(f.spanCache.nows, span.Series)
	}
}

func (f *GatherFilter) FlushExpiredSpans(now time.Time, out chan Span) {
	f.spanCache.Lock()
	for _, span := range f.spanCache.spans {
		f.FlushIfExpired(span, now, out)
	}
	f.spanCache.Unlock()
}

func (f *GatherFilter) FlushStuckSpans(out chan Span) {
	f.spanCache.Lock()
	for series, span := range f.spanCache.spans {
		willExpireAt := span.End.Add(time.Duration(f.GatherConfig.SpanWidth) * time.Second)

		if willExpireAt.After(f.lastDate) {
			f.flushSpan(span, out)
			delete(f.spanCache.spans, series)
			delete(f.spanCache.nows, series)
		}
	}
	f.spanCache.Unlock()
}

func (f *GatherFilter) PrintSpansInMem() {
	fmt.Println("Spans in mem")
	f.spanCache.Lock()
	for series, span := range f.spanCache.spans {
		willExpireAt := span.End.Add(time.Duration(f.GatherConfig.SpanWidth) * time.Second)

		fmt.Println(series)
		fmt.Println("start", span.Start)
		fmt.Println("end", span.End)
		fmt.Println("now", f.spanCache.nows[span.Series])
		fmt.Println("expires", willExpireAt)
		fmt.Println("")
	}
	f.spanCache.Unlock()
}

func (f *GatherFilter) flushSpan(span *Span, out chan Span) {
	span.Duration = span.End.Sub(span.Start) // + (time.Duration(f.GatherConfig.SampleInterval) * time.Second)
	err := span.CalcScore(f.aggregator)
	if err != nil {
		fmt.Println(err)
		return
	}
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
