(mongodb-cdc)=
# Relay MongoDB Change Stream into CrateDB

## About
[mongodb_cdc_cratedb.py] demonstrates a basic example program to relay event
records from [MongoDB Change Streams] into [CrateDB].

> Change streams allow applications to access real-time data changes without the prior
> complexity and risk of manually tailing the oplog. Applications can use change streams
> to subscribe to all data changes on a single collection, a database, or an entire
> deployment, and immediately react to them.
>
> - https://www.mongodb.com/docs/manual/changeStreams/
> - https://www.mongodb.com/developer/languages/python/python-change-streams/


## Services

### CrateDB
Start CrateDB.
```shell
docker run --rm -it --name=cratedb --publish=4200:4200 --env=CRATE_HEAP_SIZE=2g \
    crate:5.7 -Cdiscovery.type=single-node
```

### MongoDB
Start MongoDB.
Please note that change streams are only available for replica sets and
sharded clusters, so let's define a replica set by using the
`--replSet rs-testdrive` option when starting the MongoDB server.
```shell
docker run -it --rm --name=mongodb --publish=27017:27017 \
    mongo:7 mongod --replSet rs-testdrive
```

Now, initialize the replica set, by using the `mongosh` command to invoke
the `rs.initiate()` operation.
```shell
export MONGODB_URL="mongodb://localhost/"
docker run -i --rm --network=host mongo:7 mongosh ${MONGODB_URL} <<EOF

config = {
    _id: "rs-testdrive",
    members: [{ _id : 0, host : "localhost:27017"}]
};
rs.initiate(config);

EOF
```


## Install
Acquire and set up the basic relay program.
```shell
# Install dependencies.
pip install 'commons-codec[mongodb]' pymongo sqlalchemy-cratedb

# Download program.
wget https://github.com/daq-tools/commons-codec/raw/main/examples/mongodb_cdc_cratedb.py
```


## Usage

Configure settings.
```shell
export CRATEDB_SQLALCHEMY_URL="crate://"
export MONGODB_URL="mongodb://localhost/"
```

Invoke relay program.
```shell
python mongodb_cdc_cratedb.py cdc-relay
```

Invoke database workload.
```shell
python mongodb_cdc_cratedb.py db-workload
```


## Troubleshooting

When you see this message on MongoDB's server log, it indicates you tried to
configure a replica set, but did not initialize it yet.
```text
pymongo.errors.OperationFailure: The $changeStream stage is only supported on
replica sets, full error: {'ok': 0.0, 'errmsg': 'The $changeStream stage is
only supported on replica sets', 'code': 40573, 'codeName': 'Location40573'}
```

When you see a `Failed to refresh key cache` error message on MongoDB's server
log, it indicates the server has been successfully running a replica set last
time, but, again, it has not been correctly initialized.

- https://stackoverflow.com/questions/70518350/mongodb-replicaset-failed-to-refresh-key-cache
- https://www.mongodb.com/community/forums/t/how-to-recover-mongodb-from-failed-to-refresh-key-cache/239079


[CrateDB]: https://github.com/crate/crate
[mongodb_cdc_cratedb.py]: https://github.com/daq-tools/commons-codec/raw/main/examples/mongodb_cdc_cratedb.py
[MongoDB Change Streams]: https://www.mongodb.com/docs/manual/changeStreams/
