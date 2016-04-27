package hekaanom

import (
	"crypto/md5"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/mozilla-services/heka/pipeline"
)

var Algos = []string{"RPCA"}

const (
	DefaultAlgo = "RPCA"
)

type Detector interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in chan Window) chan Ruling
	PrintQs()
}

type DetectConfig struct {
	Algorithm      string                `toml:"algorithm"`
	MaxProcs       int                   `toml:"max_procs"`
	DetectorConfig pipeline.PluginConfig `toml:"config"`
}

type DetectAlgo interface {
	Init(config interface{}) error
	Detect(win Window) Ruling
}

type DetectFilter struct {
	Detectors []DetectAlgo
	*DetectConfig
	chans     []chan Window
	seriesToI map[string]int
}

func (f *DetectFilter) ConfigStruct() interface{} {
	return &DetectConfig{
		Algorithm: DefaultAlgo,
		MaxProcs:  runtime.GOMAXPROCS(0),
	}
}

func (f *DetectFilter) Init(config interface{}) error {
	f.DetectConfig = config.(*DetectConfig)

	if f.DetectConfig.Algorithm == "" {
		return errors.New("No 'algorithm' specified.")
	}
	if !algoIsKnown(f.DetectConfig.Algorithm) {
		return errors.New("Unknown algorithm.")
	}
	f.Detectors = make([]DetectAlgo, f.DetectConfig.MaxProcs)
	switch f.DetectConfig.Algorithm {
	case "RPCA":
		for i := 0; i < f.DetectConfig.MaxProcs; i++ {
			f.Detectors[i] = new(RPCADetector)
			if err := f.Detectors[i].Init(f.DetectConfig.DetectorConfig); err != nil {
				return err
			}
		}
	}
	f.seriesToI = make(map[string]int, f.DetectConfig.MaxProcs)
	f.chans = make([]chan Window, f.DetectConfig.MaxProcs)

	return nil
}

func (f *DetectFilter) PrintQs() {
	for i, ch := range f.chans {
		fmt.Println(i, " - ", len(ch))
	}
	fmt.Println()
}

func (f *DetectFilter) Connect(in chan Window) chan Ruling {
	var wg sync.WaitGroup
	out := make(chan Ruling)
	wg.Add(f.DetectConfig.MaxProcs)

	detect := func(detector DetectAlgo, in chan Window, out chan Ruling) {
		for window := range in {
			out <- detector.Detect(window)
		}
		wg.Done()
	}

	for i := 0; i < f.DetectConfig.MaxProcs; i++ {
		f.chans[i] = make(chan Window, 10000)
		go detect(f.Detectors[i], f.chans[i], out)
	}

	go func() {
		defer close(out)
		for window := range in {
			i, ok := f.seriesToI[window.Series]
			if !ok {
				i = f.seriesIndex(window.Series, f.DetectConfig.MaxProcs-1)
				f.seriesToI[window.Series] = i
			}
			f.chans[i] <- window
		}
		wg.Wait()
		return
	}()

	return out
}

func iFromHash(series string, maxI int) int {
	checksum := md5.Sum([]byte(series))
	sum := 0
	for _, b := range checksum {
		sum += int(b)
	}
	return sum % (maxI + 1)
}

func (f *DetectFilter) seriesIndex(series string, maxI int) int {
	i := iFromHash(series, maxI)
	min := 10000
	for j, ch := range f.chans {
		if len(ch) < min {
			i = j
			min = len(ch)
		}
	}
	return i
}

func algoIsKnown(algo string) bool {
	for _, v := range Algos {
		if v == algo {
			return true
		}
	}
	return false
}
