# pg-exporter

**High-performance PostgreSQL export framework for TypeScript.**

`pg-exporter` streams data directly from PostgreSQL and exports it to text-based formats such as **CSV** and **fixed-length records**. It is designed for large datasets, scheduled exports, reporting, ETL pipelines, banking files, and enterprise data integration.

Instead of loading all rows into memory, `pg-exporter` processes records one at a time using PostgreSQL streaming, making it suitable for exporting millions of rows efficiently.

### Examples:
- [postgres-export-sample](https://github.com/typescript-sample/postgres-export-sample): export data from PostgreSQL to CSV.

---

# Features

* PostgreSQL streaming export
* Constant memory usage
* Schema-based field mapping
* Pluggable query builders
* Pluggable formatters
* Pluggable writers
* Progress logging
* Generic TypeScript APIs
* Functional and object-oriented APIs
* Built for enterprise batch processing

---

# Why pg-exporter?

Exporting large datasets usually requires solving several independent problems:

* Building SQL queries
* Reading rows efficiently
* Mapping database columns
* Formatting records
* Writing output files

`pg-exporter` separates these responsibilities into reusable components.

```text
 Query Builder
       │
       ▼
PostgreSQL Stream
       │
       ▼
 Field Mapping
       │
       ▼
   Formatter
       │
       ▼
    Writer
       │
       ▼
  Output File
```

Each component has a single responsibility, making export pipelines easier to understand, test, and maintain.

---

# Architecture

```text
        PostgreSQL
             │
             ▼
       QueryBuilder
             │
             ▼
     PostgreSQL Stream
             │
   Optional Field Mapping
             │
             ▼
         Formatter
             │
             ▼
           Writer
             │
             ▼
       Exported File
```

The exporter orchestrates the pipeline while each component focuses on one task:

* **QueryBuilder** builds SQL statements.
* **Formatter** converts rows into text.
* **Writer** persists formatted output.
* **Exporter** coordinates the entire export process.

---

# Streaming Architecture

Instead of executing a query and loading every record into memory:

```text
     SELECT
        │
        ▼
 Millions of Rows
        │
        ▼
Large Memory Usage
```

`pg-exporter` streams one row at a time.

```text
PostgreSQL
    │
    ▼
 One Row
    │
    ▼
Formatter
    │
    ▼
  Writer
    │
    ▼
 Next Row
```

This architecture keeps memory usage nearly constant regardless of dataset size.

---

# Flexible Query Generation

SQL generation is separated from the exporter.

```text
 Application
      │
      ▼
 QueryBuilder
      │
      ▼
SQL Statement
      │
      ▼
   Exporter
```

Applications can build SQL dynamically without changing export logic.

---

# Field Mapping

Database column names do not always match application models.

`pg-exporter` supports attribute-based mapping between database fields and exported objects.

```text
Database Column
 customer_name
       │
       ▼
Application Model
  customerName
```

This keeps SQL independent from application naming conventions.

---

# Formatter Abstraction

The exporter does not know anything about CSV, fixed-length files, or other output formats.

It simply calls:

```text
     Row
      │
      ▼
  Formatter
      │
      ▼
Formatted Text
```

Any formatter implementing the interface can be used.

Typical examples include:

* CSV
* Fixed-length records
* JSON Lines
* Custom enterprise formats

This makes formatting completely reusable.

---

# Writer Abstraction

Writing output is separated from formatting.

```text
Formatted Text
      │
      ▼
    Write
      │
      ▼
     Disk
```

The writer may write to:

* Files
* Memory
* Network streams
* Cloud storage
* Any custom destination

---

# Progress Logging

Long-running exports can report progress at configurable intervals.

```text
  10,000 Rows
      │
      ▼
 Progress Log
      │
      ▼
Continue Export
```

This provides visibility during large batch jobs while avoiding excessive logging.

---

# Two Programming Styles

## Functional API

Applications may provide simple callback functions.

```text
buildQuery()

format()

write()
```

This approach minimizes boilerplate and works well for lightweight applications.

---

## Object-Oriented API

Applications may also implement dedicated components.

```text
QueryBuilder

Formatter

Writer
```

This style integrates naturally with dependency injection and enterprise architectures.

Both approaches share the same export pipeline.

---

# Typical Use Cases

* Scheduled database exports
* CSV generation
* Fixed-length file generation
* Regulatory reporting
* Banking integrations
* ETL pipelines
* Data warehouse exports
* Batch processing
* Data migration
* Enterprise reporting

---

# Design Principles

`pg-exporter` is built around a few core principles:

* Streaming over buffering
* Composition over inheritance
* Small, focused interfaces
* Separation of responsibilities
* Pluggable components
* Constant memory usage
* Enterprise-ready architecture

---

# Integration with export-kit

`pg-exporter` focuses on retrieving data from PostgreSQL.

Formatting is delegated to **export-kit**, allowing the same formatter implementations to be reused across different export workflows.

```text
PostgreSQL
     │
     ▼
pg-exporter
     │
     ▼
 export-kit
     │
     ▼
CSV / Fixed-Length File
```

This separation keeps database access independent from output formatting.

---

# When to Use pg-exporter

Choose `pg-exporter` when you need to:

* Export large PostgreSQL tables
* Generate CSV reports efficiently
* Produce fixed-length files from PostgreSQL
* Build reusable export pipelines
* Stream millions of records with low memory usage
* Separate SQL generation, formatting, and file writing

If your application requires scalable, maintainable, and reusable PostgreSQL export pipelines, `pg-exporter` provides the building blocks without imposing a heavyweight framework.

# Ecosystem Integration

Several [**core-ts**](https://github.com/core-ts) libraries can work together.

| Library                                                     | Purpose                             |
|-------------------------------------------------------------|-------------------------------------|
| [`config-plus`](https://www.npmjs.com/package/config-plus)  | Configuration management            |
| [`logger-core`](https://www.npmjs.com/package/logger-core)  | Structured logging                  |
| [`export-kit`](https://www.npmjs.com/package/export-kit)    | File I/O, CSV and fixed-length formatting |
| [`onecore`](https://www.npmjs.com/package/onecore)          | Unified Metadata          |

Each library focuses on a single responsibility.

That demonstrates the intended layering very well.

# License

MIT License.
