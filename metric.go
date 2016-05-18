package hekaanom

import (
	"time"

	"github.com/mozilla-services/heka/message"
)

type Metric struct {
	Timestamp   time.Time
	Series      string
	Value       float64
	Passthrough []*message.Field
}
