package hekaanom

import (
	"crypto/md5"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/mozilla-services/heka/pipeline"
)

var algos = []string{"RPCA"}

const defaultAlgo = "RPCA"

type detector interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in chan window) chan ruling
	PrintQs()
	QueuesEmpty() bool
}

type DetectConfig struct {
	Algorithm      string                `toml:"algorithm"`
	MaxProcs       int                   `toml:"max_procs"`
	DetectorConfig pipeline.PluginConfig `toml:"config"`
}

type detectAlgo interface {
	Init(config interface{}) error
	Detect(win window, out chan ruling)
}

type detectFilter struct {
	Detectors []detectAlgo
	*DetectConfig
	chans     []chan window
	seriesToI map[string]int
}

func (f *detectFilter) ConfigStruct() interface{} {
	return &DetectConfig{
		Algorithm: defaultAlgo,
		MaxProcs:  runtime.GOMAXPROCS(0),
	}
}

func (f *detectFilter) Init(config interface{}) error {
	f.DetectConfig = config.(*DetectConfig)

	if f.DetectConfig.Algorithm == "" {
		return errors.New("No 'algorithm' specified.")
	}
	if !algoIsKnown(f.DetectConfig.Algorithm) {
		return errors.New("Unknown algorithm.")
	}
	f.Detectors = make([]detectAlgo, f.DetectConfig.MaxProcs)
	switch f.DetectConfig.Algorithm {
	case "RPCA":
		for i := 0; i < f.DetectConfig.MaxProcs; i++ {
			f.Detectors[i] = new(rPCADetector)
			if err := f.Detectors[i].Init(f.DetectConfig.DetectorConfig); err != nil {
				return err
			}
		}
	}
	f.seriesToI = make(map[string]int, f.DetectConfig.MaxProcs)
	f.chans = make([]chan window, f.DetectConfig.MaxProcs)

	return nil
}

func (f *detectFilter) QueuesEmpty() bool {
	for _, length := range f.QueueLengths() {
		if length > 0 {
			return false
		}
	}
	return true
}

func (f *detectFilter) QueueLengths() []int {
	lengths := make([]int, len(f.chans))
	for i, ch := range f.chans {
		lengths[i] = len(ch)
	}
	return lengths
}

func (f *detectFilter) PrintQs() {
	for i, length := range f.QueueLengths() {
		fmt.Println(i, " - ", length)
	}
	fmt.Println()
}

func (f *detectFilter) Connect(in chan window) chan ruling {
	var wg sync.WaitGroup
	out := make(chan ruling)
	wg.Add(f.DetectConfig.MaxProcs)

	detect := func(detector detectAlgo, in chan window, out chan ruling) {
		for window := range in {
			detector.Detect(window, out)
		}
		wg.Done()
	}

	for i := 0; i < f.DetectConfig.MaxProcs; i++ {
		f.chans[i] = make(chan window, 10000)
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

func (f *detectFilter) seriesIndex(series string, maxI int) int {
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
	for _, v := range algos {
		if v == algo {
			return true
		}
	}
	return false
}
