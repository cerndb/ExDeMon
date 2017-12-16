# Statuses management

In order to perform the computation in same of the components, previous activity needs to be stored. This information is stored in a object that we call "status".

You may want to list the current statuses in order to understand the results or remove the status of, for example, a specific host.

Statuses are stored in an external system for easy management.

## Storage system

TODO

## Removing statuses

The application can be configured to listen to a TCP socket from which JSON documents will be collected.

JSON documents should represent status keys. These keys will be removed.

```
statuses.removal.socket = <host:port>
```

## Statuses management: list and remove

TODO
