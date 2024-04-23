# Synthetic data generator
Author: David Salac

## Context
Synthetic data are critical for the development of the system. 
They allow testing system features (from a development and QA
perspective) and see tangible outcomes (from a customer's
perspective). This repository contains a stand-alone script for
generating synthetic data (for the Microsoft SQL Server
database), all inputs needed are included.

## Software User Manual
This section describes what needs to be configured in order
to run the script and how to run it.

### Environmental variables
Script is controlled through following environmental variables:
1. `SYNTHGEN_SQL_STRUCTURE_PATH`:
  Path to the CSV, which defines the structure of the database,the outcome of:
```sql
SELECT
    TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
    ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS;
```
2. `SYNTHGEN_MAX_ROWS_PER_TABLE`: 
  Maximal number of rows generated to each table
3. `SYNTHGEN_GENERATE_CREATE_STATEMENTS`: 
  If set to 1, CREATE statements are included, if 0 then no.
4. `SYNTHGEN_GENERATE_INSERT_STATEMENTS`: 
   If set to 1, INSERT statements are included, if 0 then no.
5. `SYNTHGEN_NUMBER_ROWS_PER_TABLE_PATH`: 
  Path to the JSON file that contains the number of rows per each table
  following the logic: `{ "FULL_TABLE_NAME": NUMBER, ... }`
  It is a transformed outcome of the query:
```sql
SELECT
    s.name AS SCHEMA_NAME, t.name AS TABLE_NAME, p.rows AS NR_ROWS
FROM sys.tables t, sys.partitions p, sys.schemas s
WHERE t.object_id = p.object_id and s.schema_id = t.schema_id;
```
6. `SYNTHGEN_MAX_STRING_SIZE`: 
  Maximal size of generated strings (for varchar-like column).
7. `SYNTHGEN_MAX_BINARY_ARRAY_SIZE`:
  Maximal size of generated binary arrays (for binary-like column).
8. `SYNTHGEN_INTEGER_MAXIMUM`:
  Maximum values for generated random integers
9. `SYNTHGEN_OUTPUT_FILE`:
  Either the path where the output should be generated OR
  if not set, the output is printed to standard output.
10. `SYNTHGEN_NULL_PROBABILITY_PERCENT`:
  Percentage probability that the nullable value is NULL.
11. `PRIMARY_KEYS_PATH`: Path to the JSON with primary keys
  definitions (logic: table name -> col 1, col 2, ...)

None of these variables needs to be set up in order to run
the script as the default value can be used.

**Note:** `SYNTHGEN_OUTPUT_FILE` variable has a functional impact.

### Requirements
Python in version 3.10 or higher (CPython). No additional
packages are required.

### Running of the script
Script can be easily invoked using the following command:
```shell
python synthetic_data_generator.py
```
or equivalent.

### Expected runtime
With the default configuration the script should finish in around
3 seconds.
