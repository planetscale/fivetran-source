---
name: PlanetScale
title: PlanetScale Database Platform | Fivetran implementation docs
description: Fivetran for PlanetScale documentation includes a setup guide and more. Learn how our data pipelines make it easy for analysts to use SQL or analytics tools.
---

# PlanetScale {% badge text="Partner-Built" /%} {% availabilityBadge connector="planetscale" /%}

[PlanetScale](https://planetscale.com) is a MySQL database platform built on top of Vitess, the popular open-source database management technology created at YouTube. Vitess enables horizontal sharding of MySQL abstracted from the application layer. It’s designed to improve database management and provide a performant, fault-tolerant database that can handle large workloads.

PlanetScale provides customers with the power of Vitess, offering a fully managed and performant MySQL database service with horizontal sharding, Git-like schema change workflows, automatic backup, recovery, advanced query analytics, and multi-region replication capabilities. PlanetScale can be deployed on multiple cloud platforms, including Amazon Web Services (AWS) and Google Cloud Platform (GCP).

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to PlanetScale connector and its documentation, contact PlanetScale's support team. For details on SLA, see [PlanetScale's Support Overview documentation](https://planetscale.com/docs/support/support-overview).

----

## Features

{% featureTable connector="planetscale" /%}

----

## Setup guide

Follow the [step-by-step PlanetScale setup guide](/docs/connectors/databases/planetscale/setup-guide) to connect your PlanetScale database with Fivetran.

----

## Sync overview

Once Fivetran is connected to your PlanetScale primary or read replica, we pull a complete dump of all selected data from your database. We then connect to your database's [VStream](https://vitess.io/docs/17.0/concepts/vstream/) to pull all your new and changed data at regular intervals. VStream is Vitess' change tracking mechanism. If data in the source changes (for example, you add new tables or change a data type), Fivetran automatically detects and persists these changes into your destination.


### Syncing empty tables and columns

Fivetran can sync empty tables and columns for your PlanetScale connection. For more information, see our [Features documentation](/docs/using-fivetran/features#syncingemptytablesandcolumns).

### Safe migrations 

We recommend you enable the [safe migrations](https://planetscale.com/docs/concepts/safe-migrations) option in PlanetScale. This highly recommended feature ensures schema changes are performed without downtime, lock contention, or data loss.

----

## Schema information

Fivetran tries to replicate the exact schema and tables from your PlanetScale source database to your destination according to our [standard database update strategies](/docs/connectors/databases#transformationandmappingoverview). For every schema in the PlanetScale database you connect, we create a schema in your destination that maps directly to its native schema. This ensures that the data in your destination is in a familiar format to work with.

### Fivetran-generated columns

Fivetran adds the following columns to every table in your destination:

- `_fivetran_deleted` (BOOLEAN) marks deleted rows in the source database.
- `_fivetran_synced` (UTC TIMESTAMP) indicates when Fivetran last successfully synced the row.
- `_fivetran_index` (INTEGER) shows the order of updates for tables that do not have a primary key.
- `_fivetran_id` (STRING) is the hash of the non-Fivetran values of each row. It's a unique ID that Fivetran uses to avoid duplicate rows in tables that do not have a primary key.

We add these columns to give you insight into the state of your data and the progress of your data syncs. For more information about these columns, see [our System Columns and Tables documentation](/docs/core-concepts/system-columns-and-tables).

### Type transformations and mapping

As we extract your data, we match MySQL data types in your PlanetScale database to types that Fivetran supports. If we don't support a specific data type, we automatically change that type to the closest supported type or, for some types, don't load that data at all. Our system automatically skips columns with data types we don't accept or transform.

The following table illustrates how we transform your MySQL data types into Fivetran supported types:

| MySQL Type | Fivetran Data Type | Fivetran Supported | Notes |
| - | - | - | -- |
| BINARY | BINARY | True |
| BIGINT | LONG | True |
| BIT | BOOLEAN | True | BIT type with a single digit is supported. |
| BLOB | BINARY | True |
| CHAR | STRING | True |
| DATE | DATE | True | Invalid values will be loaded as NULL or EPOCH if the type is a primary key. |
| DATETIME | TIMESTAMP_NTZ | True | Invalid values will be loaded as NULL or EPOCH if the type is a primary key. |
| DECIMAL/ NUMERIC | BIGDECIMAL | True |
| DOUBLE | DOUBLE | True |
| ENUM | STRING | True |
| FLOAT | DOUBLE | True |
| GEOMETRY | JSON | True |
| GEOMETRYCOLLECTION | JSON | True |
| JSON | JSON | True |
| INT | INTEGER | True |
| LINESTRING | JSON | True |
| LONGBLOB | BINARY | True |
| LONGTEXT | STRING | True |
| MEDIUMBLOB | BINARY | True |
| MEDIUMINT | INTEGER | True |
| MEDIUMTEXT | STRING | True |
| MULTILINESTRING | JSON | True |
| MULTIPOINT | JSON | True |
| MULTIPOLYGON | JSON | True |
| POINT | JSON | True |
| POLYGON | JSON | True |
| SET | JSON | True |
| SMALLINT | INTEGER | True |
| TIME | STRING | True |
| TIMESTAMP | TIMESTAMP | True | MYSQL always stores timestamps in UTC. Invalid values will be loaded as NULL or EPOCH if the type is a primary key. |
| TINYBLOB | BINARY | True |
| TINYINT | BOOLEAN | True | If you select `Treat TinyInt(1) as boolean` in the connection configuration, we will enforce that the tinyint is either 1 or 0 and return true/false accordingly. |
| TINYINT | INTEGER | True | In all other cases, the destination type for TINYINT columns will be INTEGER. If the width isn't specified to be exactly 1 (either no specification or a value other than 1), the destination type will be INTEGER, even if the column contains only 1s or 0s. |
| TINYTEXT | STRING | True |
| UNSIGNED BIGINT | BIGDECIMAL | True |
| UNSIGNED INT | LONG | True |
| UNSIGNED SMALLINT | INTEGER | True |
| VARCHAR | STRING | True |
| VARBINARY | BINARY | True |
| YEAR | INTEGER | True |

If we are missing an important data type that you need, [reach out to support](https://support.fivetran.com/).

In some cases, when loading data into your destination, we may need to convert Fivetran data types into data types supported by the destination. For more information, see the [individual data destination pages](/docs/destinations).

### Unparsable values

When we encounter [an unparsable value](/docs/connectors/databases/mysql#unparsablevalues) of one of the following data types, we substitute it with a default value. Which default value we use depends on whether the unparsable value is in a primary key column or non-primary key column:

| MySQL Type | Primary Key Value | Non-Primary Key Value |
| - | - | - |
| DATE | 1970-01-01 | null |
| DATETIME | 1970-01-01T00:00:00  | null |
| TIMESTAMP | 1970-01-01T00:00:00Z | null |

Although we may be able to read some values outside the supported DATE, DATETIME, and TIMESTAMP ranges as defined by [MySQL's documentation](https://dev.mysql.com/doc/refman/8.0/en/datetime.html), there is no guarantee. Additionally, the special zero value 0000-00-00 00:00:00 is subject to this rule.

### Excluding source data

If you don’t want to sync all the data from your primary database, you can exclude schemas, tables, or columns from your syncs on your Fivetran dashboard. To do so, go to your connection details page and uncheck the objects you want to omit from syncing. 

For more information, see our [Data Blocking documentation](/docs/using-fivetran/features/data-blocking-column-hashing).

----

## Initial sync

When Fivetran connects to a new database, we first copy all rows from every table in every schema for which we have SELECT permission (except those you have excluded in your Fivetran dashboard) and add [Fivetran-generated columns](/docs/connectors/databases/mysql#fivetrangeneratedcolumns). Tables are copied in ascending size order (from smallest to largest). We copy rows by performing a SELECT statement on each table. For large tables, we copy a limited number of rows at a time so that we don't have to start the sync again from the beginning if our connection is lost midway.

The duration of initial syncs can vary depending on the number and size of tables to be imported. We, therefore, interleave incremental updates with the table imports during the initial sync.

----

## Updating data

Fivetran performs incremental updates of any new or modified data from your source database. We use Vitess's inbuilt VStream VGtids, which allows Fivetran to update only the data that has changed since our last sync.

### Deleted rows

We don't delete rows from the destination. We handle deletes as part of streaming changes from VStream. Note that we only process `DELETE` events from the stream.

### Deleted columns

We don't delete columns from your destination. When a column is deleted from the source table, we replace the existing values in the corresponding destination column with NULL values.

### Adding and dropping columns

When you add or drop a column, we attempt to migrate your destination schema to the new table structure automatically. In some cases, we won't be able to do this and instead perform an automatic re-sync of the changed table.

In the following scenarios, Fivetran will re-sync your table instead of automatically migrating it:

- Changing column order
- Changing primary keys
- Modifying ENUM or SET columns
