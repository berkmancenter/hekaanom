package hekaanom

import "github.com/mozilla-services/heka/message"

type Ruling struct {
	Window        Window
	Anomalous     bool
	Anomalousness float64
	Normed        float64
}

func (r Ruling) fillMessage(m *message.Message) error {
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
	m.AddField(anomalous)
	m.AddField(anomalousness)
	m.AddField(normed)
	return nil
}
