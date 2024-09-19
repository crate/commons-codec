# Changelog

## Unreleased

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
