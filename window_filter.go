package hekaanom

import (
	"errors"
	"time"

	"github.com/mozilla-services/heka/pipeline"
)

type Windower interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in chan Metric, out chan Window) error
}

type WindowConfig struct {
	WindowWidth int64 `toml:"window_width"`
}

type WindowFilter struct {
	windows map[string]*Window
	*WindowConfig
}

func (f *WindowFilter) ConfigStruct() interface{} {
	return &WindowConfig{}
}

func (f *WindowFilter) Init(config interface{}) error {
	f.WindowConfig = config.(*WindowConfig)
	if f.WindowConfig.WindowWidth <= 0 {
		return errors.New("'window_width' setting must be greater than zero.")
	}
	f.windows = map[string]*Window{}
	return nil
}

func (f *WindowFilter) Connect(in chan Metric, out chan Window) error {
	for metric := range in {
		window, ok := f.windows[metric.Series]
		if !ok {
			window = &Window{
				Start:  metric.Timestamp,
				Series: metric.Series,
			}
			f.windows[metric.Series] = window
		}

		windowAge := metric.Timestamp.Sub(window.Start)
		if int64(windowAge/time.Second) >= f.WindowConfig.WindowWidth {
			f.flushWindow(window, out)
			window.Start = metric.Timestamp
		}

		window.Value += metric.Value
		window.End = metric.Timestamp
	}
	return nil
}

func (f *WindowFilter) flushWindow(window *Window, out chan Window) error {
	out <- *window
	*window = Window{Series: window.Series}
	return nil
}
