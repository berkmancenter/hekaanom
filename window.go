package hekaanom

import (
	"errors"
	"time"

	"github.com/mozilla-services/heka/message"
)

const TimeFormat = time.RFC3339Nano

type Window struct {
	Start  time.Time
	End    time.Time
	Series string
	Value  float64
}

func WindowFromMessage(m *message.Message) (Window, error) {
	start, ok := m.GetFieldValue("window_start")
	if !ok {
		return Window{}, errors.New("Message does not contain 'window_start' field")
	}
	end, ok := m.GetFieldValue("window_end")
	if !ok {
		return Window{}, errors.New("Message does not contain 'window_end' field")
	}
	series, ok := m.GetFieldValue("series")
	if !ok {
		return Window{}, errors.New("Message does not contain 'series' field")
	}
	value, ok := m.GetFieldValue("value")
	if !ok {
		return Window{}, errors.New("Message does not contain 'value' field")
	}

	startTime, err := time.Parse(TimeFormat, start.(string))
	if err != nil {
		return Window{}, err
	}
	endTime, err := time.Parse(TimeFormat, end.(string))
	if err != nil {
		return Window{}, err
	}

	return Window{startTime, endTime, series.(string), value.(float64)}, nil
}

func (w Window) FillMessage(m *message.Message) error {
	start, err := message.NewField("window_start", w.Start.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'window_start' field")
	}
	end, err := message.NewField("window_end", w.End.Format(TimeFormat), "date-time")
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
	durField, err := message.NewField("window_duration", duration.String(), "duration")
	if err != nil {
		return errors.New("Could not create 'duration' field")
	}
	m.SetTimestamp(w.End.UnixNano())
	m.AddField(start)
	m.AddField(end)
	m.AddField(series)
	m.AddField(durField)
	m.AddField(value)
	return nil
}
