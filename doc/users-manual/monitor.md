# Monitors

Monitor is the abstraction used in this application to determine what [metrics](metrics.md) should be monitored, how and when to raise notifications.

Each monitor have:
* Optionally, a [filter](metrics-filter.md) to determine what metrics should be monitor. If not filter is applied, all metrics will be processed.
* An [analysis](monitor-analysis.md) to determine the status of each metric. Default analysis always results in status OK.
* Zero or more [notificators](monitor-notificator.md), they determine when to raise notifications.

The configuration parameters can be seen below.

```
## filter (optional)
monitor.<monitor-id>.filter.expr = <predicate with () | & = !=>
monitor.<monitor-id>.filter.attribute.<metric_attribute_key> = <[!]regex_or_exact_value>
monitor.<monitor-id>.filter.attribute... (as many attributes as needed)
## analysis
monitor.<monitor-id>.analysis.type = <analysis_type> (default: none analysis)
monitor.<monitor-id>.analysis.<other_confs> = <value>
## notificators (optional)
monitor.<monitor-id>.notificator.<notificator-id-1>.type = <notificator-type>
monitor.<monitor-id>.notificator.<notificator-id-1>.sinks = <ALL|space separated notificatios sink ids> (default: ALL)
monitor.<monitor-id>.notificator.<notificator-id-1>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.notificator.<notificator-id-1>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.notificator.<notificator-id-1>.tags.<tag-key-n> = <value-n>
monitor.<monitor-id>.notificator.<notificator-id-1>.<other_confs> = <value>
monitor.<monitor-id>.notificator.<notificator-id-n>... (as many notificators as needed)
monitor.<monitor-id>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.tags.<tag-key-n> = <value-n>
```

Configuration of monitors can be updated while running.

## Tags

Each monitor or notificator can have different tags that are included in the analysis results and notifications that the monitor produces.

They could be used to later discriminate the data, to aggregate, to target notifications (email, group, system), configure sinks, ...

Monitor tags are included in the analysis results. They can be configured as:

```
monitor.<monitor-id>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.tags.<tag-key-n> = <value-n>
```

Tags at monitor level are override by notificator tags. Merged tags from monitor and notificator are included in notifications.

```
monitor.<monitor-id>.notificator.<notificator-id>.tags.<tag-key> = <value>
```

Tags used for configuring sinks are removed from the notifications.

The value of any tag can be extracted from an attribute of the analyzed metric. For that you should follow the syntax:

```
<tag-key-1> = %<metric-key>
```

## Status

In order to perform the analysis, previous activity needs to be stored. This is stored in a status.

You may want to list the current statuses in order to understand the analysis results or remove the status of, for example, a specific host.

For that, please read the [statuses management documentation](statuses-management.md).

