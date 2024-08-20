# Changelog

## Unreleased
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
- Added `BucketTransformation`, a minimal transformation engine
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
