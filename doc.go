/*
Package hekaanom implements anomaly detection in time series data for the Heka
data processing tool (hekad.readthedocs.org). It does this by providing
a filter plugin that contains set of simple tools for turning streamed data
into suitable time series, an anomaly detection algorithm, and tools for
post-processing the anomalies. Detected anomalies are then injected back in to
the Heka pipeline as messages.

Anomaly detection in this package is designed around three stages: making
incoming data into regular time series (referred to here as "windowing"),
detecting whether or not data points in the resulting time series are anomalous
("detecting"), and gathering consecutive (or roughly consecutive) anomalous
data points together into anomalous events ("gathering").

Internally, each stage also has an associated data structure. Respectively,
these are: window, ruling (i.e. each point is "ruled" anomalous or
non-anomalous), and span (i.e. this time span is anomalous). Rulings and spans
are injected into the Heka pipeline for use by subsequent stages with the types
"anom.ruling" and "anom.span".

The plugin itself has a general configuration section in the Heka configuration
file, and each stage also has its own configuation section.

Turning incoming data into time series consists of a number of steps. First,
Heka's message matcher syntax
(http://hekad.readthedocs.io/en/v0.10.0/message_matcher.html) is used to
indicate which messages in the stream contain the intended data. Second, the
time and value components of each data point are extracted from the filtered
messages. The time component is pulled from the message's timestamp, while the
`value_field`, specified in the plugin's configuration, is used as the data
point's numeric value. Third, data points are split into independent time
series based on the related message's `series_fields`. This is in place so that
data pertaining to a number of time series may be fed into Heka at the same
time. For example, if one wants to detect anomalies in the number of hourly
visits to 10 different web pages, but all the page requests are coming in in
real-time and are interspersed, the message field that contains the web page
URL could be indiciated as the `series_field`, and a time series for each
unique URL would be created. Finally, data points in each time series are
bundled together into windows of regular and configurable "width". This is
effectively downsampling to a regular interval. Right now, all value fields of
the data points that fall within a window are added together to determine the
window's value.

Time series, which now consist of a sequence of windows, are passed on to the
detect stage. The detect stage uses a configurable anomaly detection algorithm
to to determine which windows are anomalous, and by how much.  Right now, the
only algorithm included in this package is Robust Primary Component Analysis
("RPCA"). The anomaly detection algorithm creates a ruling for every window is
receives, and injects these rulings into Heka's message pipeline.

The gather stage listens to the stream of rulings and gathers consecutive
anomalous rulings together into anomalous events. Anomalous rulings need not be
strictly consecutive; instead, a configurable parameter (`span_width`) can be
used to indicate how many consecutive seconds of non-anomalous rulings must
pass before an anomalous event is considered over. Like the windowing stage,
the gather stage collects the numeric values of rulings (determined again by
the `value_field`) together into a single value. Unlike the windowing stage,
the gather stage can use a number of different operators when combining the
constituent ruling values (outlined in the config struct documentation). The
gather stage injects the generated anomalous spans into the Heka pipeline for
any further processing or output the user might wish to perform.
*/
package hekaanom
