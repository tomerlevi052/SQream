**Introduction**

Utility for benchmarking TPC-H queries over PostgreSQL database

**Compilation and Running**
python tpch_util.py --"functionality"

**How to use**

usage: tpch_util.py [-h] [--create-schema] [--load-data] [--run-benchmark] [--save-results] [--fetch-results]

Required command-line script functionalities

optional arguments:
  -h, --help       show this help message and exit
  --create-schema  Reads the schema file and creates tables in the database. Drops existing tables with the same name if
                   any.
  --load-data      Loads the TPCH .tbl files to corresponding tables
  --run-benchmark  Run queries and output the query execution time
  --save-results   Benchmark results will be saved to tpch_results table in database
  --fetch-results  Fetch the results saved in results table and print it


**Modules**

PostgreSQL database adapter for python




