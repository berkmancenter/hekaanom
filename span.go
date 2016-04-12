package hekaanom

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/mozilla-services/heka/message"
)

const TimeFormat = time.RFC3339Nano

type AnomalousSpan struct {
	Start       time.Time
	End         time.Time
	Series      string
	Aggregation float64
	Values      []float64
	Interval    int64
	Duration    time.Duration
	Score       float64
}

func (s AnomalousSpan) FillMessage(m *message.Message) error {
	start, err := message.NewField("span_start", s.Start.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'span_start' field")
	}
	end, err := message.NewField("span_end", s.End.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'span_end' field")
	}
	series, err := message.NewField("series", s.Series, "")
	if err != nil {
		return errors.New("Could not create 'series' field")
	}
	agg, err := message.NewField("aggregation", s.Aggregation, "count")
	if err != nil {
		return errors.New("Could not create 'aggregation' field")
	}
	values, err := json.Marshal(s.Values)
	if err != nil {
		return errors.New("Could not JSON encode 'values' field")
	}
	valuesField, err := message.NewField("values", string(values), "json")
	if err != nil {
		return errors.New("Could not create 'values' field")
	}
	duration := s.End.Sub(s.Start)
	durField, err := message.NewField("span_duration", duration.String(), "duration")
	if err != nil {
		return errors.New("Could not create 'duration' field")
	}
	product := float64(duration/time.Second) * s.Aggregation
	if product == 0 {
		product = float64(s.Interval) * s.Aggregation
	}
	prodField, err := message.NewField("product", product, "count")
	if err != nil {
		return errors.New("Could not create 'product' field")
	}
	m.SetTimestamp(s.End.UnixNano())
	m.AddField(start)
	m.AddField(end)
	m.AddField(series)
	m.AddField(agg)
	m.AddField(valuesField)
	m.AddField(durField)
	m.AddField(prodField)
	return nil
}
