# DFM Validator

**DFM Validator** is a Python tool for processing Parquet datasets containing JSON-formatted "manufacturing parts" data.  
It safely parses the JSON, computes unreachable hole warnings and errors, and efficiently handles large datasets using chunked reading.  

---

## Features

- Efficient **chunked reading** for large Parquet files.
- Safe **JSON parsing** with logging for invalid entries.
- Computes two flags per row:
  - `has_unreachable_hole_warning`
  - `has_unreacheable_hole_error`
- Supports **local files** and can be adapted to read from **S3**.
- Fully **unit-tested** for robustness.

---

## Installation

1. Clone the repository:

```bash
git clone https://github.com/georgetutuianu/protolabs.git
cd protolabs
