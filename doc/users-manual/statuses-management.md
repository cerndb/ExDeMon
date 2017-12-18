# Statuses management

In order to perform the computation in same of the components, previous activity needs to be stored. This information is stored in a object that we call "status".

You may want to list the current statuses in order to understand the results or remove the status of, for example, a specific host.

Statuses are stored in an external system for easy management.

## Storage system

Statuses must be stored externally in a statuses store. The following sections describe the options.

### Single file statuses store

```
spark.cern.streaming.status.storage.type = single-file
spark.cern.streaming.status.storage.path = <path> (default: /tmp/metrics-monitor-statuses/)
```

### Kafka statuses store

Topic should be configured with [log compaction](https://kafka.apache.org/documentation/#compaction).

```
spark.cern.streaming.status.storage.type = kafka
spark.cern.streaming.status.storage.topic = <topic>
spark.cern.streaming.status.storage.timeout = <period like 1s, 1m, 3h> (default: 2s)
spark.cern.streaming.status.storage.serialization = <java or json> (default: json)
```

## Removing statuses

The application can be configured to listen to a TCP socket from which JSON documents will be collected.

JSON documents should represent status keys. These keys will be removed.

```
statuses.removal.socket = <host:port>
```

## Statuses management: list and remove

A command line interface is available to manage statuses: list keys, see values and remove.

```
$SPARK_HOME/bin/spark-submit     \
      --master local     \
      --repositories https://repository.cloudera.com/artifactory/cloudera-repos/     \
      --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,org.reflections:reflections:0.9.9     \
      --class ch.cern.spark.status.storage.manager.StatusesManagerCLI     \
      target/metrics-monitor-VERSION.jar     \
	
usage: spark-statuses-manager
      -c,--conf <arg>    path to configuration file
      -id,--id <arg>     filter by status key id
      -n,--fqcn <arg>    filter by FQCN or alias
      -p,--print <arg>   print mode: java or json
      -s,--save <arg>    path to write result as JSON
```

--conf should be the path to the configuration file of the application

For filtering statuses you can use:
* --fqcn: defined-metric-key, monitor-key or notificator-key
* --id: defined metric or monitor id, for notificators: monitor-id:notificator-id

For removing statuses, statuses.removal.socket must be configured and this command line must be run where this property is pointing.



