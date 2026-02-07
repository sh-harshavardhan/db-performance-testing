# `db-performance`

**Usage**:

```console
$ db-performance [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

**Commands**:

* `mock`: Generates mock datasets that can be used...

## `db-performance mock`

Generates mock datasets that can be used for performance testing

**Usage**:

```console
$ db-performance mock [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `employee`: Create sample employee data
* `workday`: Create sample employee data in workday...
* `Create sample TPC-H data`: Command to generate TPC-H data using...
* `tpc-ds`: Create sample TPC-DS data

### `db-performance mock employee`

Create sample employee data

**Usage**:

```console
$ db-performance mock employee [OPTIONS]
```

**Options**:

* `--scale-factor INTEGER`: Number of fake employee records to generate  [default: 100000]
* `--num-of-threads INTEGER`: Number of parallel threads to use for data generation  [default: 4]
* `--export-type [parquet|csv]`: Export format for the generated data  [default: csv]
* `--target-path TEXT`: Path where the data has to be exported  [default: fake_employee_data]
* `--help`: Show this message and exit.

### `db-performance mock workday`

Create sample employee data in workday data model

**Usage**:

```console
$ db-performance mock workday [OPTIONS]
```

**Options**:

* `--export-type [parquet|csv]`: Export format for the generated data  [default: csv]
* `--target-path TEXT`: Path where the data has to be exported  [default: fake_workday_data]
* `--help`: Show this message and exit.

### `db-performance mock Create sample TPC-H data`

Command to generate TPC-H data using DuckDB&#x27;s TPC-H extension

**Usage**:

```console
$ db-performance mock Create sample TPC-H data [OPTIONS]
```

**Options**:

* `--scale-factor [1|3|10|30|100|300|1000|3000]`: Scale factor for TPC-H data generation  [default: 3]
* `--num-of-threads INTEGER`: Number of parallel threads to use for data generation  [default: 4]
* `--export-type [parquet|csv]`: Export format for the generated data  [default: csv]
* `--target-path TEXT`: Path where the data has to be exported  [default: tpc_h_data_]
* `--sql-path TEXT`: Path where the TPC-H queries will be exported  [default: /sqls/tcp_h]
* `--help`: Show this message and exit.

### `db-performance mock tpc-ds`

Create sample TPC-DS data

**Usage**:

```console
$ db-performance mock tpc-ds [OPTIONS]
```

**Options**:

* `--scale-factor [1|10|100|1000]`: Scale factor for TPC-DS data generation  [default: 1]
* `--export-type [parquet|csv]`: Export format for the generated data  [default: csv]
* `--target-path TEXT`: Path where the data has to be exported  [default: tpc_ds_data]
* `--sql-path TEXT`: Path where the TPC-DS queries will be exported  [default: sqls/tcp_ds]
* `--help`: Show this message and exit.
