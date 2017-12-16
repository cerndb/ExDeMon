# Define new metrics

Configuration of defined metrics can be updated while running.

A meta-attribute is set in the generated metrics. The meta attribute name is $defined_metric and his value the &lt;defined-metric-id&gt;. 
This attribute can later be used to filter the defined metrics in a monitor like:

```
monitor.<monitor_id>.filter.attribute.$defined_metric = <defined-metric-id>
```

Configuration:

```
metrics.define.<defined-metric-id>.value = <equation containing <variable-ids>> (default: <variable-id> if only one variable has been declared)
metrics.define.<defined-metric-id>.when = <ANY|BATCH|space separated list of metric variable-ids> (default: the first metric variable after sorting)
metrics.define.<defined-metric-id>.metrics.groupby = <not set/ALL/space separated attribute names> (default: not set)
# General filter for all metrics that update the variables (optional)
metrics.define.<defined-metric-id>.metrics.filter.expr = <predicate with () | & = !=>
metrics.define.<defined-metric-id>.metrics.filter.attribute.<attribute-name> = <value>
# Variable that represent an incoming metric
metrics.define.<defined-metric-id>.variables.<variable-id-1>.filter.expr = <predicate with () | & = !=>
metrics.define.<defined-metric-id>.variables.<variable-id-1>.filter.attribute.<attribute-name> = <value->
metrics.define.<defined-metric-id>.variables.<variable-id-1>.aggregate = <not set|sum|avg|weighted_avg|count|max|min|diff>
metrics.define.<defined-metric-id>.variables.<variable-id-1>.aggregate.attributes = <ALL|space separated list of attributes> (default: ALL)
metrics.define.<defined-metric-id>.variables.<variable-id-1>.ignore = <not set|period like 1h, 3m or 45s[, truncate d, h, m]> (default: not set)
metrics.define.<defined-metric-id>.variables.<variable-id-1>.expire = <never|period like 1h, 3m or 45s[, truncate d, h, m]> (default: 10m)
# Variable that represent a set of properties for an analysis (could serve to configure an analysis: properties_variable)
metrics.define.<defined-metric-id>.variables.<variable-id-2>.type = <analysis_type>
metrics.define.<defined-metric-id>.variables.<variable-id-2>.<analysis-conf-key-1> = <value-1>
metrics.define.<defined-metric-id>.variables.<variable-id-2>.<analysis-conf-key-n> = <value-n>
metrics.define.<defined-metric-id>.variables.<variable-id-n>...

# With different id, more metrics can be defined
```

## Produced value: the equation

New metrics can be defined. The value of these defined metrics is computed from an equation configured with the "value" parameter. This equation can have variables, these variables can represent incoming metrics or a set of properties for configuring a function. Values from several metrics can be aggregated in order to compute the value for the new metric. Equation supports grouping using (), and it applies the operator precedence and associativity rules. By default, in case only one variable is declared, value will return this variable. Equation will determine the type of the new defined metric.

Functions that can be used in the equation with expected types and the type they return:
* \+ float => float
* \- float => float
* float + float => float
* float - float => float
* float * float => float
* float / float => float
* float ^ float => float
* float > float => boolean
* float < float => boolean
* boolean && boolean => boolean
* boolean || boolean => boolean
* !boolean => boolean
* if_bool(boolean, boolean, boolean) => boolean
* if_float(boolean, float, float) => float
* if_string(boolean, string, string) => string
* any == any => boolean
* any != any => boolean
* abs(float) => float
* sqrt(float) => float
* sin(float) => float
* cos(float) => float
* tan(float) => float
* concat(string, string) => string
* trim(string) => string
* analysis(any, analysis_properties_variable) => "OK"|"WARNING"|"ERROR" (string)

Examples:

```
metrics.define.temperature_change.value = !shouldBeMonitored || analysis(temperature, ana_props) != "OK"
metrics.define.temperature_change.variables.shouldBeMonitored.filter.attribute.TYPE = "Monitoring enabled"
metrics.define.temperature_change.variables.temperature.filter.attribute.TYPE = "Temperature in Celsious"
metrics.define.temperature_change.variables.ana_props.type = recent
metrics.define.temperature_change.variables.ana_props.error.upperbound = true
metrics.define.temperature_change.variables.ana_props.error.lowerbound = true

metrics.define.directory_full.value = !shouldBeMonitored || (trim(dir) == "/tmp/") && (abs(used / capacity) > 0.8)
metrics.define.directory_full.metrics.filter.attribute.TYPE = DirReport
metrics.define.directory_full.variables.shouldBeMonitored.filter.attribute.$value_attribute = monitor_enable
metrics.define.directory_full.variables.dir.filter.attribute.$value_attribute = path
metrics.define.directory_full.variables.used.filter.attribute.$value_attribute = used_bytes
metrics.define.directory_full.variables.capacity.filter.attribute.$value_attribute = capacity_bytes
```

> TIP For debugging, values include a source attribute where equation result can be observed with a value like:
>```
># With errors
>!(var(shouldBeMonitored)=true)=false || ((trim(var(dir)=" /tmp/  ")="/tmp/" == "/tmp/")=true && (abs((var(used)=900.0 / var(capacity)={Error: no value for the last 10 minutes})={Error: in arguments})={Error: in arguments} > 0.8)={Error: in arguments})={Error: in arguments})={Error: in arguments}
>
># With successful computation
>!(var(shouldBeMonitored)=true)=false || ((trim(var(dir)=" /tmp/  ")="/tmp/" == "/tmp/")=true && (abs((var(used)=900.0 / var(capacity)=1000.0)=0.9)=0.9 > 0.8)=true)=true)=true
>```

## Definition of variables

You need to specify what the variables in your equation represent by declaring variables. Then, &lt;variable-id-X&gt; can be used in the equation. The type of a variable is determined by the aggregation operation, if no aggregation operation is applied, it can become any type in the equation.

Variables are supposed to be updated periodically. In case they are not updated, its value expires after the period of time specified with the parameter "expire". 
You can make variables to never expire configuring "expire" parameter to "never". By default, variables get expired after 10 minutes. 
If a variable expires and the variable is used for the computation, no metrics will be produced. For aggregations, individual values are removed from the aggregation if they are not updated after such period. 
In the case all the values for a given aggregated variable expire, count is 0.
You can also ignore recent metrics by specifying an "ignore" period . Metrics will be taken into account once they pass the ignore period.
Ignore and expire can be truncate by minute (m), hour (h) or day (d), so that a variable could take, for example, only data of the previuos minute:

```
# Variable only take into account metrics of the previous minute
metrics.define.<defined-metric-id>.variables.<variable-id-1>.ignore = 0m,m
metrics.define.<defined-metric-id>.variables.<variable-id-1>.expire = 1m,m
```

### Aggregations in variables

A variable could be the result of an aggregation of values. Values from all metrics that pass the specified [filter](metrics-filter.md) (and after grouping) will be aggregated. Note that in order to differentiate between metrics, attributes specified in "aggregation.attributes" will be used, by default all attributes not specified in groupby will be used. Only attributes not specified in groupby can be used in "aggregation.attributes".
This can be configured using the "aggregate" parameter, where you configure the operation to perform the aggregation. Operation determines the variable type. The maximum number of different metrics that can be aggregated is 100000, if more, oldest metrics are removed.

Aggregation operations available and the corresponding type:
* No operation => metric as generated by metric source
* sum => filter by float type and produce float
* avg => filter by float type and produce float
* weighted_avg (influence proportional with elapsed time) => filter by float type and produce float
* count_floats => filter by float type and produce float
* count_bools => filter by boolean type and produce float
* count_true => filter by boolean type and produce float
* count_false => filter by boolean type and produce float
* count_strings => filter by string type and produce float
* max => filter by float type and produce float
* min => filter by float type and produce float
* diff (with previous value) => filter by float type and produce float

## When they are generated 

The computation and further generation of a new metric will be trigger when the variables listed in the "when" parameter are updated. By default, a new metric is produced when the first (after sorting alphabetically by &lt;variable-id&gt;) declared variable is updated with a new value. Last value of the other variables will be used for the computation. You can set "when" to ANY, it will trigger the generation when any of the variables is updated. You can also set "when" to BATCH, so the generation will be triggered not by any variable updated but in every Spark Streaming batch.

> TIP for a defined metric which aggregates with count to return 0. 
> ``` 
> # Machines sent metric if running
> metrics.define.machines-running.variables.value.filter.attribute.TYPE = "running"
> metrics.define.machines-running.variables.value.aggregate = count
> ```
> If "when" parameter is set with variable names (default), a count 0 will never be produced because no metric will trigger its generation (no metrics coming, that's why count is 0). To solve this, there are two possible solutions. 
> 1. The defined metric is triggered by another variable which only serves to trigger the generation. If trigger metric (variable) does not come, no metric is generated.
> ``` 
> # to add
> metrics.define.machines-running.value = value
> metrics.define.machines-running.when = trigger
> metrics.define.machines-running.variables.trigger.filter.attribute.TYPE = "other"
> ``` 
> 2. "when" parameter is set to BATCH, so every Spark Streaming batch the computation and generation is triggered.
> ``` 
> # to add
> metrics.define.machines-running.when = BATCH
> ```

## Grouping by metric attributes

Metrics can be grouped by (e.g. machine) with the "metrics.groupby" parameter in order to apply the equation to a set of metrics. 
Group by can be set to ALL, then each metric will be treated independently. 
If group by is configured to ALL (or all attributes the metrics contain are listed) there is no attributes to differenciate metrics and aggregate them, so aggregation is done over the historical values coming from the metric.

## Status

In order to perform the computation, previous activity needs to be stored. This is stored in a status.

You may want to list the current statuses or remove them in order to stop the generation of metrics of, for example, a specific host.

For that, please read the [statuses management documentation](statuses-management.md).

## Examples

Some examples of defined metrics can be:

### Multiply all metrics by 10

```
metrics.define.all-multiply-by-10.value = value * 10
metrics.define.all-multiply-by-10.metrics.groupby = ALL
# One of the following two would be enough
metrics.define.all-multiply-by-10.variables.value.filter.attribute.INSTANCE_NAME = .*
metrics.define.all-multiply-by-10.variables.value.filter.attribute.METRIC_NAME = .*
```

In SQL would correspond:

```
SELECT all_attributes, value*10 FROM metric GROUP BY all_attributes
```

### Divide CPU usage coming from all machines by 1000

```
metrics.define.cpu-percentage = value / 1000
metrics.define.cpu-percentage.metrics.groupby = ALL
# Same effect if we specify INSTANCE_NAME = .* or not
#metrics.define.cpu-percentage.variables.value.filter.attribute.INSTANCE_NAME = .*
metrics.define.cpu-percentage.variables.value.filter.attribute.METRIC_NAME = CPU Usage Per Sec
```

In SQL would correspond:

```
SELECT all_attributes, value/1000 FROM metric GROUP BY all_attributes
```

### Compute the ratio read/write for all machines:

```
metrics.define.ratio_read_write.value = readbytes / writebytes
metrics.define.ratio_read_write.metrics.groupby = HOSTNAME
metrics.define.ratio_read_write.variables.readbytes.filter.attribute.METRIC_NAME = Read Bytes Per Sec
metrics.define.ratio_read_write.variables.writebytes.filter.attribute.METRIC_NAME = Write Bytes Per Sec
```

In SQL would correspond:

```
SELECT HOSTNAME, read.value / write.value
  FROM (SELECT HOSTNAME, value FROM metric WHERE METRIC_NAME="Read Bytes Per Sec") read 
       JOIN (SELECT HOSTNAME, value FROM metric WHERE METRIC_NAME="Write Bytes Per Sec") write
          ON  read.HOSTNAME = write.HOSTNAME;
```

### Temperature inside minus temperature outside in Fahrenheits: 

```
metrics.define.diff_temp.value = (tempinside - tempoutside) * 9/5 + 32
# We do not group by, so that we can aggregate any metrics
metrics.define.diff_temp.variables.tempinside.filter.attribute.PLACE = Living Room
metrics.define.diff_temp.variables.tempinside.filter.attribute.METRIC = Temperature
metrics.define.diff_temp.variables.tempoutside.filter.attribute.PLACE = Outside
metrics.define.diff_temp.variables.tempoutside.filter.attribute.METRIC = Temperature
```

### Compare values of production and development environments:

```
metrics.define.diff-prod-dev.value = valueprod - valuedev
metrics.define.diff-prod-dev.metrics.groupby = INSTANCE_NAME METRIC_NAME
# Metrics contain $source attribute with <metric-source-id>, it can be used to filter
metrics.define.diff-prod-dev.variables.valueprod.filter.attribute.$source = kafka-prod
metrics.define.diff-prod-dev.variables.valuedev.filter.attribute.$source = kafka-dev
```

### Aggregate metrics for all machines for each cluster and environment

All metrics that belongs to the same cluster will be averaged. They will be grouped by METRIC_NAME.
Metrics coming from the same HOSTNAME, will update its previous value in the aggregation.

``` 
metrics.define.avg-metric-per-cluster.metrics.groupby = CLUSTER_NAME METRIC_NAME
metrics.define.avg-metric-per-cluster.variables.average-value.aggregate = avg
```

### Aggregate metrics for all machines in production for each cluster

All metrics that belongs to the same cluster (groupby), name is "Read Bytes" and environment is "production" will be accumulated.
Metrics coming from the same HOSTNAME, will update its previous value in the aggregation.

``` 
metrics.define.clusterprod-read-bytes.metrics.groupby = CLUSTER_NAME
metrics.define.clusterprod-read-bytes.variables.readbytes.filter.attribute.ENVIRONMENT = production
metrics.define.clusterprod-read-bytes.variables.readbytes.filter.attribute.METRIC_NAME = Read Bytes
metrics.define.clusterprod-read-bytes.variables.readbytes.aggregate = sum
# Value parameter is optional since there is only one variable and it has the desired value
metrics.define.clusterprod-read-bytes.value = readbytes
```

### Count number of machines running per cluster

Scenario: a machine that is running, produce a metric of type "running". If the machine stops, no metric are sent.

If the machine do not send the metric after 5 minutes, its corresponding metric is removed from the aggregation. 

``` 
metrics.define.cluster-machines-running.metrics.groupby = CLUSTER_NAME
metrics.define.cluster-machines-running.when = BATCH
metrics.define.cluster-machines-running.variables.value.filter.attribute.TYPE = "running"
metrics.define.cluster-machines-running.variables.value.aggregate = count
metrics.define.cluster-machines-running.variables.value.expire = 5m
```

In SQL would correspond:

```
SELECT CLUSTER_NAME, count(value) FROM metric WHERE TYPE="running" AND now()-time<5m GROUP BY CLUSTER_NAME
```

### Count number of machines running per cluster (without "running" metric)

Scenario: a machine that is running, produce many metrics. If the machine stops, no metrics are sent.

If the machine do not send any metric after 5 minutes, its corresponding aggregated value is removed from the aggregation. 

``` 
metrics.define.cluster-machines-running.metrics.groupby = CLUSTER_NAME
metrics.define.cluster-machines-running.when = BATCH
metrics.define.cluster-machines-running.variables.value.aggregate = count
metrics.define.cluster-machines-running.variables.value.aggregate.attributes = HOSTNMAE
metrics.define.cluster-machines-running.variables.value.expire = 5m
```

In SQL would correspond:

```
SELECT CLUSTER_NAME, count(value) FROM (SELECT CLUSTER_NAME, HOSTNMAE, value FROM metric WHERE now()-time<5m) GROUP BY CLUSTER_NAME
```

### Average value from each metric during the period of the last 5 minutes 

If we group by all the attributes (groupby = ALL), each metric is treated independently. 

Under such circunstancies there is no attrbutes in the metric to differenciate metrics and aggregate them, so aggregation is done over the historical values of the metric.

``` 
metrics.define.avg-5m.metrics.groupby = ALL
metrics.define.avg-5m.variables.value.aggregate = avg
metrics.define.avg-5m.variables.value.expire = 5m
```

### Detect if a metric stop coming (missing metric)

If we group by all the attributes (groupby = ALL), each metric will be treated independently. 

We can count how many metrics arrived in the last 10 minutes. With a monitor we can check if this defined metric gets 0, that would mean the metric is not coming.

``` 
metrics.define.missing-metric.metrics.groupby = ALL
metrics.define.missing-metric.when = BATCH
metrics.define.missing-metric.variables.value.aggregate = count
metrics.define.missing-metric.variables.value.expire = 10m
```

### Check if all log lines of the previous hour have been processed

A set of machines produces logs, these logs are collected and parsed.
Each machine count the lines to later check if lines were missed.

We compute the difference of two variables when the variable report_count is updated.

real_count variable count the number of lines that were processed the previous hour.
report_count variable represents a metric that arrives once an hour, its value indicates the lines produced in the previous hour.

The results of the different is the missing lines. Later we can place a monitor to check if it is greater than 0 and then raise a notification.

``` 
metrics.define.tapeserverd-missing.value = report_count - real_count
metrics.define.tapeserverd-missing.metrics.groupby = hostname
metrics.define.tapeserverd-missing.when = report_count
metrics.define.tapeserverd-missing.variables.real_count.filter.attribute.$schema = tapeserverd
metrics.define.tapeserverd-missing.variables.real_count.aggregate = count_floats
metrics.define.tapeserverd-missing.variables.real_count.aggregate.attributes = NONE
metrics.define.tapeserverd-missing.variables.real_count.ignore = 0h,h
metrics.define.tapeserverd-missing.variables.real_count.expire = 1h,h
metrics.define.tapeserverd-missing.variables.report_count.filter.attribute.$schema = tapeserverd-count
```

Note: another metric can be defined to check if metric that reports hte number of lines is coming or not.
So you would notice of missing metrics even if the machine is completelly dwon.
