# Change Data Capture (CDC)

`commons-codec` includes CDC -> SQL transformer components for AWS DMS,
DynamoDB, and MongoDB.

:DynamoDB:
  - Blog: [Replicating CDC Events from DynamoDB to CrateDB]
  - Documentation: [DynamoDB CDC Relay for CrateDB]

:MongoDB:
  - Usage guide: [](project:#mongodb-cdc)
  - Documentation: [MongoDB CDC Relay for CrateDB]


```{toctree}
:hidden:

MongoDB <mongodb>
```



## Prior Art

- [core-cdc] by Alejandro Cora González
- [Carabas Research]


[Carabas Research]: https://lorrystream.readthedocs.io/carabas/research.html
[core-cdc]: https://pypi.org/project/core-cdc/
[DynamoDB CDC Relay for CrateDB]: https://cratedb-toolkit.readthedocs.io/io/dynamodb/cdc.html
[MongoDB CDC Relay for CrateDB]: https://cratedb-toolkit.readthedocs.io/io/mongodb/cdc.html
[Replicating CDC Events from DynamoDB to CrateDB]: https://cratedb.com/blog/replicating-cdc-events-from-dynamodb-to-cratedb
