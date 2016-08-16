package hekaanom

import (
	"errors"
	"time"

	"github.com/mozilla-services/heka/pipeline"
)

type windower interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in <-chan metric) chan window
}

type WindowConfig struct {
	WindowWidth int64 `toml:"window_width"`
}

type windowFilter struct {
	windows map[string]*window
	*WindowConfig
}

func (f *windowFilter) ConfigStruct() interface{} {
	return &WindowConfig{}
}

func (f *windowFilter) Init(config interface{}) error {
	f.WindowConfig = config.(*WindowConfig)
	if f.WindowConfig.WindowWidth <= 0 {
		return errors.New("'window_width' setting must be greater than zero.")
	}
	f.windows = map[string]*window{}
	return nil
}

func (f *windowFilter) Connect(in <-chan metric) chan window {
	out := make(chan window)
	go func() {
		defer close(out)
		for metric := range in {
			win, ok := f.windows[metric.Series]
			if !ok {
				win = &window{
					Start:       metric.Timestamp,
					Series:      metric.Series,
					Passthrough: metric.Passthrough,
				}
				f.windows[metric.Series] = win
			}

			windowAge := metric.Timestamp.Sub(win.Start)
			if int64(windowAge/time.Second) >= f.WindowConfig.WindowWidth {
				f.flushWindow(win, out)
				win.Start = metric.Timestamp
			}

			win.Value += metric.Value
			win.End = metric.Timestamp
		}
	}()
	return out
}

func (f *windowFilter) flushWindow(win *window, out chan window) error {
	// Add one window width to the end of the width because the end is exclusive
	win.End = win.End.Add(time.Duration(f.WindowConfig.WindowWidth) * time.Second)
	out <- *win
	*win = window{Series: win.Series, Passthrough: win.Passthrough}
	return nil
}
