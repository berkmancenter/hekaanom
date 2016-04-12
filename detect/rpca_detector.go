package detect

import (
	"errors"

	"github.com/berkmancenter/hekaanom"
	"github.com/berkmancenter/rpca"

	"github.com/mozilla-services/heka/pipeline"
)

type RPCADetector struct {
	majorFreq int
	minorFreq int
	autoDiff  bool
	series    map[string][]float64
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
	d.series = map[string][]float64{}
	return nil
}

func (d *RPCADetector) Detect(win hekaanom.Window) hekaanom.Ruling {
	ruling := hekaanom.Ruling{Window: win}

	d.series[win.Series] = append(d.series[win.Series], win.Value)

	if len(d.series[win.Series]) < d.minorFreq {
		return ruling
	}

	if len(d.series[win.Series]) > d.minorFreq {
		d.series[win.Series] = d.series[win.Series][1:]
	}

	anoms := rpca.FindAnomalies(d.series[win.Series], rpca.Frequency(d.majorFreq), rpca.AutoDiff(d.autoDiff))
	i := len(anoms.Values) - 1
	anomalous, anomalousness := anoms.Positions[i], anoms.Values[i]
	normed := anoms.NormedValues[i]

	return hekaanom.Ruling{win, anomalous, anomalousness, normed}
}
