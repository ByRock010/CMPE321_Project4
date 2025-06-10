# Dune Archive System

A minimal disk-based relational DBMS implemented in Python. This project provides basic data definition and manipulation operations using a slotted-page file format.

## Features

* **Create Type (DDL)**: Define a new record type (table) with up to 6 fields, specifying field names, types (`int` or `str`), and a primary key.
* **Create Record (DML)**: Insert fixed-length records into on-disk pages. Handles integer and string fields (strings are fixed 20 bytes, padded or truncated).
* **Search Record**: Perform a linear scan by primary key to locate and retrieve a record’s field values.
* **Delete Record**: Mark a record as deleted and update the corresponding page metadata.
* **Logging**: All operations are logged (`log.csv`) with timestamps and success/failure status.
* **Output**: Search results are written to `output.txt` in chronological order.
* **Page Cap**: Enforces a maximum of 1,000 pages per data file to prevent unbounded growth.

## Repository Structure

```
2022400051_2022400282_DuneArchive/
├─ archive.py           # Main Python script
├─ catalog.meta         # System catalog (initially empty)
├─ input.txt            # Example commands file (user-provided)
├─ output.txt           # Search results output (auto-generated)
├─ log.csv              # Operation log file (auto-generated)
├─ report.pdf           # Project report document
├─ README.md            # This file
└─ tests/               # (Optional) directory for test command files
```

## Requirements

* Python 3.6 or higher

## Usage

1. Ensure you are in the project directory:

   ```bash
   cd path/to/2022400051_2022400282_DuneArchive
   ```

2. Create or clear the catalog file (automatic on first run):

   ```bash
   touch catalog.meta
   ```

3. Prepare an input file (`input.txt`) containing one command per line. Example:

   ```text
   create type house 6 1 name str origin str leader str military_strength int wealth int spice_production int
   create record house Atreides Arrakis Paul 5000 10000 300
   search record house Atreides
   delete record house Atreides
   ```

4. Run the archive system:

   ```bash
   python3 archive.py input.txt
   ```

5. View results:

   * **Search output**: `output.txt`
   * **Operation log**: `log.csv`

## Design Overview

* **Slotted‑Page Format**: Each `.dat` file is a sequence of fixed-size pages (11-byte header + 10 record slots). Slots contain a 1-byte valid flag and field bytes.
* **Fixed‑Length Records**: Integers occupy 4 bytes; strings occupy 20 bytes. Simplifies offset calculations.
* **Primary Key Lookup**: Sequential scan across pages and slots (O(pages × slots))—acceptable for small datasets.
* **No Deallocation**: Pages remain allocated even if empty. Uni-directional append-only for simplicity.
* **Page Cap**: `MAX_PAGES = 1000` enforced before appending new pages.

## Testing

* Add test command files to `tests/` and run:

  ```bash
  python3 archive.py tests/your_test_file.txt
  ```

  Verify `output.txt` and `log.csv` for expected behavior.

## Contribution Reports

Individual contribution reports (`<StudentID> Contribution.pdf`) must be included in the submission ZIP:

* `2022400051 Contribution.pdf` (Ahmet Baha Bayrakçıoğlu)
* `2022400282 Contribution.pdf` (Arif Evren)
