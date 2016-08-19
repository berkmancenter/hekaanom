hekaanom
========

[![GoDoc](https://godoc.org/github.com/berkmancenter/hekaanom?status.png)](https://godoc.org/github.com/berkmancenter/hekaanom)

**Note:** Mozilla [has stated](https://mail.mozilla.org/pipermail/heka/2016-May/001059.html) that they intend to stop maintaining Heka. This filter will continue being a useful first pass to look for anomalies in data, but it should not be used as a long-term production tool.

hekaanom is a Go library implementing anomaly detection in time series data as a filter plugin for the [Heka data processing tool](https://hekad.readthedocs.org).

More documentation is available via [godoc](https://godoc.org/github.com/berkmancenter/hekaanom).

### Getting started

To get Heka running with this filter installed, you'll have to build Heka yourself. But it's pretty easy. We'll assume a Unix-y environment here, but Windows will work too (look at [Heka's docs](http://hekad.readthedocs.io/en/v0.10.0/installing.html) for Windows instructions).

1. First, install the [prerequesites](http://hekad.readthedocs.io/en/v0.10.0/installing.html#from-source) for Heka
2. Check out the Heka repository: `git clone https://github.com/mozilla-services/heka`
3. Tell Heka to include the hekaanom filter: `echo 'add_external_plugin(git https://github.com/berkmancenter/hekaanom v1.0.0)' >> heka/cmake/plugin_loader.cmake`
4. Build heka: `cd heka; source build.sh`
5. Write a [config file](http://hekad.readthedocs.io/en/v0.10.0/config/index.html) that sets up data inputs, outputs, and configures this filter
6. Run heka: `build/heka/bin/hekad -config {config file}`

More documentation on the build process is available in [Heka's docs](http://hekad.readthedocs.io/en/v0.10.0/installing.html).

### Example Configuration

The full list of configurable options is available in the [godoc documentation](https://godoc.org/github.com/berkmancenter/hekaanom#AnomalyConfig), but here is an example config section to start from when building your Heka config file:

```toml
[anom_filter]
type = "AnomalyFilter"
message_matcher = "Type == 'my.metric' && Fields[rate] == 'daily'"
value_field = "views"
series_fields = ["page", "country"]
ticker_interval = 5 # seconds
realtime = false

  [anom_filter.window]
  window_width = 86400 # seconds = 1 day

  [anom_filter.detect]
  algorithm = "RPCA"

    [anom_filter.detect.config]
    major_frequency = 7 # days = 1 week
    minor_frequency = 56 # days = 8 weeks
    autodiff = false

  [anom_filter.gather]
  span_width = 345600 # seconds = 4 days
  last_date = "yesterday"
  statistic = "Mean"
  value_field = "Normed"
```

The indentation isn't necessary, but helps illustrate the conceptual nesting of the configuration.

### License

Copyright 2016 President and Fellows of Harvard College

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
