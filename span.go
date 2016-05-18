package hekaanom

import (
	"errors"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/mozilla-services/heka/message"
)

type Span struct {
	Start       time.Time
	End         time.Time
	Series      string
	Aggregation float64
	Values      []float64
	Duration    time.Duration
	Score       float64
	Passthrough []*message.Field
}

func (span *Span) CalcScore(agg func(stats.Float64Data) (float64, error)) error {
	span.trimValues()
	if len(span.Values) == 1 {
		span.Aggregation = span.Values[0]
	} else {
		aggregation, err := agg(span.Values)
		if err != nil {
			return err
		}
		span.Aggregation = aggregation
	}
	span.Score = float64(span.Duration/time.Second) * span.Aggregation
	return nil
}

func (span *Span) trimValues() {
	// We want to keep zeroes if they occur between two non-zero values. Walk
	// backward through the list.
	trimmedVals := []float64{}
	keepZeroes := false
	for i := len(span.Values) - 1; i >= 0; i-- {
		val := span.Values[i]
		if val != 0.0 {
			keepZeroes = true
		}
		if keepZeroes || val != 0.0 {
			trimmedVals = append(trimmedVals, val)
		}
	}
	// Reverse them so we can set them to the span values in correct order.
	for l, r := 0, len(trimmedVals)-1; l < r; l, r = l+1, r-1 {
		trimmedVals[l], trimmedVals[r] = trimmedVals[r], trimmedVals[l]
	}
	span.Values = trimmedVals
}

func (s Span) FillMessage(m *message.Message) error {
	start, err := message.NewField("start", s.Start.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'start' field")
	}
	m.AddField(start)

	end, err := message.NewField("end", s.End.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'end' field")
	}
	m.AddField(end)

	series, err := message.NewField("series", s.Series, "")
	if err != nil {
		return errors.New("Could not create 'series' field")
	}
	m.AddField(series)

	agg, err := message.NewField("aggregation", s.Aggregation, "count")
	if err != nil {
		return errors.New("Could not create 'aggregation' field")
	}
	m.AddField(agg)

	valuesField := message.NewFieldInit("values", message.Field_DOUBLE, "count")
	for _, val := range s.Values {
		err := valuesField.AddValue(val)
		if err != nil {
			return errors.New("Could not create 'values' field")
		}
	}
	m.AddField(valuesField)

	durField, err := message.NewField("duration", s.Duration.Seconds(), "seconds")
	if err != nil {
		return errors.New("Could not create 'duration' field")
	}
	m.AddField(durField)

	score, err := message.NewField("score", s.Score, "count")
	if err != nil {
		return errors.New("Could not create 'score' field")
	}
	m.AddField(score)

	for _, field := range s.Passthrough {
		m.AddField(field)
	}

	m.SetTimestamp(s.End.UnixNano())
	return nil
}
