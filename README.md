# Reconcilation CLI  tools 
**1. Introduction**

This document describes how to use the `reconciii` command-line tool to compare two CSV files and generate a report highlighting missing records and discrepancies between them. The tool can be helpful for data validation, identifying discrepancies in data pipelines, and ensuring data consistency across multiple sources.

**2. Prerequisites**

* Python 3.6+
* `click` package installed (pip install click)
* Pandas library installed (pip install pandas)
* Levenshtein library installed (pip install Levenshtein)

**2.1 Installing**
```bash
    pip3 install reconciii
```
**3. Running the Tool**

**3.1 Required arguments:**

* `-s` or `--source`: Path to the source CSV file.
* `-t` or `--target`: Path to the target CSV file.
* `-o` or `--output`: Path to the output CSV file containing the reconciliation report (default: "reconciliation_report.csv").

**3.2 Optional arguments:**

* `-c` or `--comparison-columns`: List of additional columns to compare for discrepancies (default: all columns except "ID").

**3.2 Example usage:**

``` bash
 reconcile -s data/source.csv -t data/target.csv -o report.csv -c column1,column2
```



This command will compare the source file `data/source.csv` with the target file `data/target.csv`, generate a reconciliation report named `report.csv`, and compare discrepancies in columns `column1` and `column2` in addition to the default "ID" column.

**4. Output Report**

The generated report will be a CSV file with the following columns:

* `Type`: Indicates the type of discrepancy (missing in source, missing in target, field discrepancy).
* `Record Identifier`: ID of the record where the discrepancy was found.
* `Field`: (optional) Specific field where the discrepancy was found (applicable for field discrepancies).
* `Source Value`: Value of the field in the source file.
* `Target Value`: Value of the field in the target file.
