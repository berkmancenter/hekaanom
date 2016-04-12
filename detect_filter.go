package hekaanom

import (
	"errors"

	"github.com/mozilla-services/heka/pipeline"
)

var Algos = []string{"RPCA"}

type Detector interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in chan Window, out chan Ruling) error
}

type DetectConfig struct {
	Algorithm      string                `toml:"algorithm"`
	DetectorConfig pipeline.PluginConfig `toml:"config"`
}

type DetectAlgo interface {
	Init(config interface{}) error
	Detect(win Window) Ruling
}

type DetectFilter struct {
	Detector DetectAlgo
	*DetectConfig
}

func (f *DetectFilter) ConfigStruct() interface{} {
	return &DetectConfig{Algorithm: "RPCA"}
}

func (f *DetectFilter) Init(config interface{}) error {
	f.DetectConfig = config.(*DetectConfig)

	if f.DetectConfig.Algorithm == "" {
		return errors.New("No 'algorithm' specified.")
	}
	if !algoIsKnown(f.DetectConfig.Algorithm) {
		return errors.New("Unknown algorithm.")
	}
	switch f.DetectConfig.Algorithm {
	case "RPCA":
		f.Detector = new(RPCADetector)
	}
	return f.Detector.Init(f.DetectConfig.DetectorConfig)
}

func (f *DetectFilter) Connect(in chan Window, out chan Ruling) error {
	for window := range in {
		ruling := f.Detector.Detect(window)
		out <- ruling
	}
	return nil
}

func algoIsKnown(algo string) bool {
	for _, v := range Algos {
		if v == algo {
			return true
		}
	}
	return false
}
