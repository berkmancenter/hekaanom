package hekaanom

import "time"

type Metric struct {
	Timestamp time.Time
	Series    string
	Value     float64
}
