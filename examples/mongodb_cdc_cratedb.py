"""
Basic example relaying a MongoDB Change Stream into CrateDB table.

Documentation:
- https://github.com/daq-tools/commons-codec/blob/main/doc/mongodb.md
- https://www.mongodb.com/docs/manual/changeStreams/
- https://www.mongodb.com/developer/languages/python/python-change-streams/
"""

import datetime as dt
import os
import sys

import pymongo
import sqlalchemy as sa
from commons_codec.transform.mongodb import MongoDBCDCTranslatorCrateDB


class MiniRelay:
    """
    Relay MongoDB Change Stream into CrateDB table, and provide basic example workload generator.
    """

    def __init__(
        self,
        mongodb_url: str,
        mongodb_database: str,
        mongodb_collection: str,
        cratedb_sqlalchemy_url: str,
        cratedb_table: str,
    ):
        self.cratedb_client = sa.create_engine(cratedb_sqlalchemy_url, echo=True)
        self.mongodb_client = pymongo.MongoClient(mongodb_url)
        self.mongodb_collection = self.mongodb_client[mongodb_database][mongodb_collection]
        self.table_name = cratedb_table
        self.cdc = MongoDBCDCTranslatorCrateDB(table_name=self.table_name)

    def start(self):
        """
        Subscribe to change stream events, convert to SQL, and submit to CrateDB.
        """
        with self.cratedb_client.connect() as connection:
            connection.execute(sa.text(self.cdc.sql_ddl))
            for sql in self.cdc_to_sql():
                if sql:
                    connection.execute(sa.text(sql))
                    connection.execute(sa.text(f'REFRESH TABLE "{self.table_name}";'))

    def cdc_to_sql(self):
        """
        Subscribe to change stream events, and emit corresponding SQL statements.
        """
        # Note that `.watch()` will block until events are ready for consumption, so
        # this is not a busy loop. Also note that the routine doesn't perform any sensible
        # error handling yet.
        while True:
            with self.mongodb_collection.watch(full_document="updateLookup") as change_stream:
                for change in change_stream:
                    print("MongoDB Change Stream event:", change, file=sys.stderr)
                    yield self.cdc.to_sql(change)

    def db_workload(self):
        """
        Run insert_one, update_one, and delete_one operations to generate a very basic workload.
        """
        example_record = {
            "id": "5F9E",
            "data": {"temperature": 42.42, "humidity": 84.84},
            "meta": {"timestamp": dt.datetime.fromisoformat("2024-07-12T01:17:42+02:00"), "device": "foo"},
        }

        print(self.mongodb_collection.insert_one(example_record))
        # print(self.mongodb_collection.update_one({"id": "5F9E"}, {"$set": {"data": {"temperature": 42.50}}}))

        # TODO: Investigate: When applying the "replace" operation, subsequent "delete" operations
        #       will not be reported to the change stream any longer. Is it a bug?
        # print(self.mongodb_collection.replace_one({"id": "5F9E"}, {"tags": ["deleted"]}))

        # print(self.mongodb_collection.delete_one({"id": "5F9E"}))

        # Drop operations are ignored anyway.
        # print(self.mongodb_collection.drop())


if __name__ == "__main__":
    # Decode subcommand from command line argument.
    if len(sys.argv) < 2:
        raise ValueError("Subcommand missing. Accepted subcommands: subscribe, workload")
    subcommand = sys.argv[1]

    # Configure machinery.
    relay = MiniRelay(
        mongodb_url=os.environ["MONGODB_URL"],
        mongodb_database="testdrive",
        mongodb_collection="data",
        cratedb_sqlalchemy_url=os.environ["CRATEDB_SQLALCHEMY_URL"],
        cratedb_table="cdc-testdrive",
    )

    # Invoke machinery.
    if subcommand == "cdc-relay":
        relay.start()
    elif subcommand == "db-workload":
        relay.db_workload()
    else:
        raise ValueError("Accepted subcommands: subscribe, workload")
