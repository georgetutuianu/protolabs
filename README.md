# DFM Validator

**DFM Validator** is a Python tool for processing Parquet datasets containing JSON-formatted "manufacturing parts" data.  
It safely parses the JSON, computes unreachable hole warnings and errors, and efficiently handles large datasets using chunked reading.  

---

## Features

- Efficient **chunked reading** for large Parquet files.
- Safe **JSON parsing** with logging for invalid entries.
- Computes two flags per row:
  - `has_unreachable_hole_warning`
  - `has_unreachable_hole_error`
- Supports **local files** and can be adapted to read from **S3**.
- Fully **unit-tested** for robustness.

---

## Installation

1. Clone the repository:

```bash
git clone https://github.com/georgetutuianu/protolabs.git
cd protolabs
```

2. Create a virtual environment:
```bash
 python -m venv venv
# Linux/Mac
source venv/bin/activate
# Windows
venv\Scripts\activate

```

3. Install dependencies
```bash
 pip install -r requirements.txt
```

## Run the project
```bash
python run_validator.py <path_to_input_file> <path_to_output_file>
```

## Testing
```bash
pytest tests/ --maxfail=1 --disable-warnings -v
```

## Sample dataset results
```text
rows with holes information = 10211

has_unreachable_hole_warning = 326
has_unreachable_hole_error = 39

rows with errors but no warnings = 0
```

## Result of the project
The result assumes the parquet files will be written back to a filesystem (local / s3). And the new rows will be only
uuid, has_unreachable_hole_warning, has_unreachable_hole_error. I chose this path only to take into account the idea of 
having large datasets where it would be inefficient to have everything in memory in python. 

With this approach, in order gain access to all the existing fields + the warn/error fields created it would require
to have a join in the dataset (by uuid) at a later step, during the query phase.

## Assumptions
- For the missing holes this will return `False` for both warn & error
- The validations are the basic ones. Assuming there's also a schema defined for the data and the validation of the 
schema would happen in another step
- As mentioned above, new columns can be appended only in a query step with this approach. It's fairly easy if we want
read and write the full dataset, with all the columns
- There will be a mechanism in place for knowing which files were already processed and which were not when running this
transformation pipeline
- If we store the bad records in another place, it assumes the existence of another mechanism that would resend the
data through the pipeline

## Next steps for preparing it for production

- Create a separate flow for bad rows
- Add a linter for static code validation like `ruff` [https://github.com/astral-sh/ruff]
- Improve it to be able to read and write to S3 (AWS) or any other data storage needed
- Integrate it into a pipeline (Airflow / Prefect / anything else)
- Create a Github pipeline for running the static and unit tests and maybe even automated deployment
- Add monitoring and alerting
