package hekaanom

import "github.com/mozilla-services/heka/message"

type ruling struct {
	Window        window
	Anomalous     bool
	Anomalousness float64
	Normed        float64
	Passthrough   []*message.Field
}

func (r ruling) FillMessage(m *message.Message) error {
	r.Window.FillMessage(m)

	anomalous, err := message.NewField("anomalous", r.Anomalous, "")
	if err != nil {
		return err
	}
	anomalousness, err := message.NewField("anomalousness", r.Anomalousness, "count")
	if err != nil {
		return err
	}
	normed, err := message.NewField("normed", r.Normed, "count")
	if err != nil {
		return err
	}

	m.AddField(anomalousness)
	m.AddField(normed)
	m.AddField(anomalous)

	for _, field := range r.Passthrough {
		m.AddField(field)
	}
	return nil
}
