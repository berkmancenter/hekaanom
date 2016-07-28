package hekaanom

import (
	"errors"

	"github.com/berkmancenter/rpca"

	"github.com/mozilla-services/heka/pipeline"
)

type RPCADetector struct {
	majorFreq int
	minorFreq int
	autoDiff  bool
	series    map[string][]*Window
}

func (d *RPCADetector) Init(config interface{}) error {
	conf := config.(pipeline.PluginConfig)

	majorFreq, ok := conf["major_frequency"]
	if !ok {
		return errors.New("Must provide 'major_frequency'")
	}
	d.majorFreq = int(majorFreq.(int64))
	if d.majorFreq <= 0 {
		return errors.New("'major_frequency' must be >= 0")
	}

	minorFreq, ok := conf["minor_frequency"]
	if !ok {
		return errors.New("Must provide 'minor_frequency'")
	}
	d.minorFreq = int(minorFreq.(int64))
	if d.minorFreq <= 0 {
		return errors.New("'minor_frequency' must be >= 0")
	}

	if d.minorFreq%d.majorFreq > 0 {
		return errors.New("'minor_frequency' must be divisible by 'major_frequency'")
	}

	autoDiff, ok := conf["autodiff"]
	if !ok {
		autoDiff = true
	}
	d.autoDiff = autoDiff.(bool)
	d.series = map[string][]*Window{}
	return nil
}

func (d *RPCADetector) Detect(win Window, out chan Ruling) {

	d.series[win.Series] = append(d.series[win.Series], &win)
	series := d.series[win.Series]

	if len(series) < d.minorFreq {
		return
	}

	// If this completes our window, send all the anomalies we haven't been
	// sending up to now.
	sendAll := false
	if len(series) == d.minorFreq {
		sendAll = true
	}

	if len(series) > d.minorFreq {
		d.series[win.Series] = series[1:]
	}

	values := make([]float64, len(d.series[win.Series]))
	for i, thisWin := range d.series[win.Series] {
		values[i] = thisWin.Value
	}

	anoms := rpca.FindAnomalies(values, rpca.Frequency(d.majorFreq), rpca.AutoDiff(d.autoDiff))

	if sendAll {
		for i := range anoms.Positions {
			out <- Ruling{
				Window:        *series[i],
				Anomalous:     anoms.Positions[i],
				Anomalousness: anoms.Values[i],
				Normed:        anoms.NormedValues[i],
				Passthrough:   series[i].Passthrough,
			}
		}
	} else {
		// Just send the latest anomaly
		i := len(anoms.Values) - 1
		anomalous, anomalousness := anoms.Positions[i], anoms.Values[i]
		normed := anoms.NormedValues[i]
		out <- Ruling{win, anomalous, anomalousness, normed, win.Passthrough}
	}
}
