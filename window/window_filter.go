package window

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mozilla-services/heka/pipeline"

	"github.com/berkmancenter/hekaanom"
)

type Windower interface {
	pipeline.HasConfigStruct
	pipeline.Plugin
	Connect(in chan hekaanom.Metric, out chan hekaanom.Window) error
}

type WindowConfig struct {
	WindowWidth int64 `toml:"window_width"`
}

type WindowFilter struct {
	windows map[string]*hekaanom.Window
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
	f.windows = map[string]*hekaanom.Window{}
	return nil
}

func (f *WindowFilter) Connect(in chan hekaanom.Metric, out chan hekaanom.Window) error {
	for metric := range in {
		window, ok := f.windows[metric.Series]
		if !ok {
			window = &hekaanom.Window{Series: metric.Series}
			f.windows[metric.Series] = window
		}

		if window.Start.IsZero() {
			window.Start = metric.Timestamp
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

func (f *WindowFilter) flushWindow(window *hekaanom.Window, out chan hekaanom.Window) error {
	out <- window
	*window = hekaanom.Window{Series: window.Series}
	return nil
}
