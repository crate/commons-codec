# Changelog

## Unreleased

## 2025/07/01 v0.0.24
- DMS: Added option `ignore_ddl` to ignore DDL events
- DMS: Added direct column mapping
- DMS: Started using direct column mapping by default
- DMS: Started using the mapping value `OBJECT` when defining column types

## 2025/06/23 v0.0.23
- Dependencies: Migrated from `zyp` to `tikray`. It's effectively the
  same, but provided using a dedicated package now
- CI: Added support for Python 3.13
- DMS: Fixed handling of primary keys
- DMS: Added support for the previously missing `drop-table` operation
- DMS: Fixed primary key / where clause rendering

## 2024/10/28 v0.0.22
- DynamoDB/Testing: Use CrateDB nightly again
- DynamoDB: Use `ON CONFLICT DO NOTHING` clause on CDC operations
  of type `INSERT`, to mitigate errors when events are relayed
  redundantly from retries after partially failed batches on the
  Lambda processor.

## 2024/10/09 v0.0.21
- MongoDB: Fixed BSON decoding of `{"$date": 1180690093000}` timestamps
- DynamoDB/Testing: Use CrateDB 5.8.3 for because 5.8.4 can no longer
  store `ARRAY`s with varying inner types into `OBJECT(IGNORED)` columns.

## 2024/09/30 v0.0.20
- DynamoDB: Change CrateDB data model to use (`pk`, `data`, `aux`) columns
  Attention: This is a breaking change.
- MongoDB: Handle too large `$date.$numberLong` values gracefully
- Zyp/Moksha: Improve error reporting when rule evaluation fails

## 2024/09/26 v0.0.19
- DynamoDB CDC: Fix `MODIFY` operation by propagating `NewImage` fully
- Zyp/Moksha: Improve error reporting when rule evaluation fails

## 2024/09/25 v0.0.18
- MongoDB: Improved `MongoDBCrateDBConverter.decode_canonical` to also
  decode non-UUID binary values
- Zyp/Moksha/jq: `to_object` function now respects a `zap` option, that
  removes the element altogether if it's empty
- Zyp/Moksha/jq: Improve error reporting at `MokshaTransformation.apply`
- MongoDB: Improved `MongoDBCrateDBConverter.decode_extended_json`

## 2024/09/22 v0.0.17
- MongoDB: Fixed edge case when decoding MongoDB Extended JSON elements
- Zyp: Added capability to skip rule evaluation when `disabled: true`

## 2024/09/19 v0.0.16
- MongoDB: Added `MongoDBFullLoadTranslator` and `MongoDBCrateDBConverter`
- Zyp: Fixed execution of collection transformation
- Zyp: Added software test and documentation about flattening lists
- MongoDB: Use `bson` package to parse BSON CANONICAL representation
- MongoDB: Complete and verify BSON data type mapping end-to-end
- MongoDB: Use improved decoding machinery also for `MongoDBCDCTranslator`
- Dependencies: Make MongoDB subsystem not strictly depend on Zyp
- Zyp: Translate a few special treatments to jq-based `MokshaTransformation` again
- Zyp: Improve documentation
- Packaging: Add missing `MANIFEST.in`

## 2024/09/10 v0.0.15
- Added Zyp Treatments, a slightly tailored transformation subsystem

## 2024/09/02 v0.0.14
- Replace poor man's relation name quoting with implementation
  `quote_relation_name` from `sqlalchemy-cratedb` package.
- DynamoDB: Add special decoding for varied lists, storing them into a separate
  `OBJECT(IGNORED)` column in CrateDB
- DynamoDB: Improve `to_sql()` to accept list of records

## 2024/08/27 v0.0.13
- DMS/DynamoDB: Use parameterized SQL WHERE clauses instead of inlining values

## 2024/08/26 v0.0.12
- DMS/DynamoDB/MongoDB: Use SQL with parameters instead of inlining values

## 2024/08/23 v0.0.11
- DynamoDB: Fix serializing OBJECT and ARRAY representations to CrateDB

## 2024/08/22 v0.0.10
- DynamoDB: Fix `Map` representation to CrateDB.

## 2024/08/22 v0.0.9
- DynamoDB: Fix `String Set` and `Number Set` representation for CrateDB
- DynamoDB: Fix serializing empty strings

## 2024/08/20 v0.0.8
- DynamoDB: Apply rough type evaluation and dispatching when computing
  values for `UPDATE` statements

## 2024/08/17 v0.0.7
- DynamoDB: Fixed a syntax issue with `text` data type in `UPDATE` statements

## 2024/08/16 v0.0.6
- Changed `UPDATE` statements from DMS not to write the entire `data`
  column. This allows defining primary keys on the sink table.

## 2024/08/16 v0.0.5
- Changed `UPDATE` statements from DynamoDB not to write the entire `data`
  column. This allows defining primary keys on the sink table.

## 2024/08/14 v0.0.4
- Added Zyp Transformations, a minimal transformation engine
  based on JSON Pointer (RFC 6901).
- Added documentation using Sphinx and Read the Docs

## 2024/08/05 v0.0.3
- Added transformer for AWS DMS to CrateDB SQL
- Dropped support for Python 3.7

## 2024/07/19 v0.0.2
- Added transformer for MongoDB CDC to CrateDB SQL conversion
- Rename "Airrohr" decoder to "Sensor.Community"

## 2024/07/16 v0.0.1
- Added decoders for Airrohr, Tasmota, and TTS/TTN from Kotori DAQ
- Added transformer for DynamoDB CDC to CrateDB SQL conversion
