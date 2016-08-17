package hekaanom

import (
	"errors"
	"time"

	"github.com/mozilla-services/heka/message"
)

type window struct {
	Start       time.Time
	End         time.Time
	Series      string
	Value       float64
	Passthrough []*message.Field
}

func windowFromMessage(m *message.Message) (window, error) {
	start, ok := m.GetFieldValue("window_start")
	if !ok {
		return window{}, errors.New("Message does not contain 'window_start' field")
	}
	end, ok := m.GetFieldValue("window_end")
	if !ok {
		return window{}, errors.New("Message does not contain 'window_end' field")
	}
	series, ok := m.GetFieldValue("series")
	if !ok {
		return window{}, errors.New("Message does not contain 'series' field")
	}
	value, ok := m.GetFieldValue("value")
	if !ok {
		return window{}, errors.New("Message does not contain 'value' field")
	}

	startTime, err := time.Parse(timeFormat, start.(string))
	if err != nil {
		return window{}, err
	}
	endTime, err := time.Parse(timeFormat, end.(string))
	if err != nil {
		return window{}, err
	}

	return window{startTime, endTime, series.(string), value.(float64), nil}, nil
}

func (w window) FillMessage(m *message.Message) error {
	start, err := message.NewField("window_start", w.Start.Format(timeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'window_start' field")
	}
	end, err := message.NewField("window_end", w.End.Format(timeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'window_end' field")
	}
	series, err := message.NewField("series", w.Series, "")
	if err != nil {
		return errors.New("Could not create 'series' field")
	}
	value, err := message.NewField("value", w.Value, "count")
	if err != nil {
		return errors.New("Could not create 'value' field")
	}
	duration := w.End.Sub(w.Start)
	durField, err := message.NewField("window_duration", duration.Seconds(), "seconds")
	if err != nil {
		return errors.New("Could not create 'duration' field")
	}
	m.SetTimestamp(w.End.UnixNano())
	m.AddField(series)
	m.AddField(start)
	m.AddField(end)
	m.AddField(durField)
	m.AddField(value)

	return nil
}
