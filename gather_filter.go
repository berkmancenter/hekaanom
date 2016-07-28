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
	// Is gathering anomalies into spans disabled?
	Disabled bool `toml:"disabled"`

	// If two anomalies occur within SpanWidth seconds of one another (i.e. their
	// ends are less than or equal to SpanWidth seconds apart), they're gathered
	// into the same anomalous span.
	SpanWidth int64 `toml:"span_width"`

	// Statistic is used to describe the anomalous span in one number derived
	// from the ValueField's of the gathered anomalies. Possible values are
	// "Sum", "Mean", "Median", "Midhinge", and "Trimean".
	Statistic string

	// ValueField identifies the field of each anomaly that should be used to
	// generate their parent span's statistic.
	ValueField string `toml:"value_field"`

	// LastDate is the date and time of the final piece of data you're
	// processing. We use this to close out the last span.
	LastDate string `toml:"last_date"`
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
		Disabled:   false,
		Statistic:  DefaultAggregator,
		ValueField: DefaultValueField,
	}
}

func (f *GatherFilter) Init(config interface{}) error {
	f.GatherConfig = config.(*GatherConfig)

	if f.GatherConfig.Disabled {
		return nil
	}

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

	// There are four things that can be happening here:
	//     We can have an active span and get non-anomalous, in which case we expire it or add it to the span.
	//     We can have an active span and get anomalous, in which case we add it to the span and extend the span's lifespan.
	//     We can not have an active span and get a non-anomalous, in which case we do nothing.
	//     We can not have an active span and get anomalous, in which case we make a new span.
	// We always update the time and expire spans.

	go func() {
		defer close(out)

		for ruling := range in {
			thisSeries := ruling.Window.Series

			f.spanCache.Lock()

			// Update the time for the current series.
			now := ruling.Window.End
			f.spanCache.nows[thisSeries] = now

			value, err := f.getRulingValue(ruling)
			if err != nil {
				fmt.Println(err)
				f.spanCache.Unlock()
				continue
			}

			// Does a span already exist for the current series?
			span, ok := f.spanCache.spans[thisSeries]
			if ok {
				if ruling.Anomalous {
					// This ruling is anomalous, so add it to this span and extend the
					// span's lifespan.
					span.Values = append(span.Values, value)
					span.End = now
				} else {
					// This ruling is not anomalous. If this span is expired, flush it.
					// If it's not, add this ruling but don't extend its lifespan.
					if f.SpanExpired(span, now) {
						f.FlushSpan(span, out)
					} else {
						span.Values = append(span.Values, value)
					}
				}
			} else if ruling.Anomalous {
				// This ruling is anomalous, so start a new span.
				span = &Span{
					Series:      thisSeries,
					Values:      []float64{value},
					Start:       ruling.Window.Start,
					End:         ruling.Window.End,
					Passthrough: ruling.Window.Passthrough,
				}
				f.spanCache.spans[thisSeries] = span
			}

			f.spanCache.Unlock()
		}
	}()
	return out
}

func (f *GatherFilter) SpanExpired(span *Span, now time.Time) bool {
	// When will this span be too old?
	willExpireAt := span.End.Add(time.Duration(f.GatherConfig.SpanWidth) * time.Second)

	isExpired := now.After(willExpireAt)

	// Are we never going to get enough data to expire this span naturally?
	outOfData := willExpireAt.Equal(f.lastDate) || willExpireAt.After(f.lastDate)

	return isExpired || outOfData
}

func (f *GatherFilter) FlushSpan(span *Span, out chan Span) {
	// Only called from within a goroutine that already locks spanCache for
	// writing, so we don't need to lock here.
	f.flushSpan(span, out)
	delete(f.spanCache.spans, span.Series)
	delete(f.spanCache.nows, span.Series)
}

func (f *GatherFilter) FlushExpiredSpans(now time.Time, out chan Span) {
	f.spanCache.Lock()
	for _, span := range f.spanCache.spans {
		if f.SpanExpired(span, now) {
			f.FlushSpan(span, out)
		}
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
