package hekaanom

import (
	"errors"
	"time"

	"github.com/mozilla-services/heka/message"
)

type Bin struct {
	Start   time.Time
	End     time.Time
	Count   int64
	Entries []string
}

type Bins map[time.Time]*Bin

func (b *Bin) FillMessage(m *message.Message) error {
	start, err := message.NewField("bin_start", b.Start.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'bin_start' field")
	}

	end, err := message.NewField("bin_end", b.End.Format(TimeFormat), "date-time")
	if err != nil {
		return errors.New("Could not create 'bin_end' field")
	}

	count, err := message.NewField("count", b.Count, "count")
	if err != nil {
		return errors.New("Could not create 'count' field")
	}

	seriesStr := ""
	for i, series := range b.Entries {
		seriesStr += series
		if i < len(b.Entries)-1 {
			seriesStr += ", "
		}
	}
	if err != nil {
		return errors.New("Could not JSON encode 'series' field")
	}
	seriesField, err := message.NewField("series", string(seriesStr), "json")
	if err != nil {
		return errors.New("Could not create 'series' field")
	}

	m.SetTimestamp(b.Start.UnixNano())
	m.AddField(start)
	m.AddField(end)
	m.AddField(count)
	m.AddField(seriesField)
	return nil
}
