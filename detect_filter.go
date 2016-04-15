package hekaanom

import (
	"errors"
	"runtime"

	"github.com/mozilla-services/heka/pipeline"
)

var Algos = []string{"RPCA"}

type Detector interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in <-chan Window, out chan<- Ruling, done <-chan struct{})
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
	Detectors []DetectAlgo
	*DetectConfig
	numQueues     int
	queues        []chan Window
	seriesToQueue map[string]int
	nextQueue     int
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
	f.numQueues = runtime.GOMAXPROCS(0) - 1
	f.seriesToQueue = make(map[string]int)
	f.Detectors = make([]DetectAlgo, f.numQueues)
	switch f.DetectConfig.Algorithm {
	case "RPCA":
		for i := 0; i < f.numQueues; i++ {
			f.Detectors[i] = new(RPCADetector)
		}
	}
	var err error
	for i := 0; i < f.numQueues; i++ {
		err = f.Detectors[i].Init(f.DetectConfig.DetectorConfig)
		if err != nil {
			return err
		}
	}
	f.queues = make([]chan Window, f.numQueues)
	return nil
}

func (f *DetectFilter) Connect(in <-chan Window, out chan<- Ruling, done <-chan struct{}) {
	for i := 0; i < f.numQueues; i++ {
		f.queues[i] = make(chan Window, 10000)
		go func(i int, in <-chan Window, out chan<- Ruling) {
			for window := range in {
				ruling := f.Detectors[i].Detect(window)
				select {
				case out <- ruling:
				case <-done:
					println("detect")
					return
				}
			}
		}(i, f.queues[i], out)
	}

	go func() {
		defer close(out)
		for window := range in {
			queue := f.queueForSeries(window.Series)
			select {
			case f.queues[queue] <- window:
			case <-done:
				return
			}
		}
	}()
}

func (f *DetectFilter) queueForSeries(key string) int {
	q, ok := f.seriesToQueue[key]
	if ok {
		return q
	}

	q = f.nextQueue
	shift := len(f.queues[q]) > int(float64(cap(f.queues[q]))*0.98)
	for i := 0; i < f.numQueues; i++ {
		if shift {
			q = (q + 1) % f.numQueues
			shift = len(f.queues[q]) > int(float64(cap(f.queues[q]))*0.98)
		}
	}
	f.nextQueue = (q + 1) % f.numQueues
	f.seriesToQueue[key] = q
	return f.seriesToQueue[key]
}

func algoIsKnown(algo string) bool {
	for _, v := range Algos {
		if v == algo {
			return true
		}
	}
	return false
}
