package hekaanom

import "github.com/mozilla-services/heka/message"

type Ruling struct {
	Window        Window
	Anomalous     bool
	Anomalousness float64
	Normed        float64
}

func (r Ruling) fillMessage(m *message.Message) error {
	anomalous, err := message.NewField("anomalous", r.anomalous, "")
	if err != nil {
		return err
	}
	anomalousness, err := message.NewField("anomalousness", r.anomalousness, "count")
	if err != nil {
		return err
	}
	normed, err := message.NewField("normed", r.normed, "count")
	if err != nil {
		return err
	}
	m.AddField(anomalous)
	m.AddField(anomalousness)
	m.AddField(normed)
	return nil
}
