# mz_parquet

*REQUEST FOR COMMENT* - If you have feedback or suggetions for this file format, it would be greatly appreciated (use the issue tracker!).

This repository houses a proof-of-concept for a modern, data lake-ready storage format for mass spectrometry data (mzparquet). A rust application is also provided for converting existing mzML files to mzparquet

Current standards like mzML, mzMLb, or mz5 have good characteristics for long-term data archival, but all require custom parsers to access data.

The Apache Parquet file format has emerged as the default file format for data science/big data workflows. Parquet allows storing nested data (such as lists of m/z and intensities, or key-value pairs of controlled vocabulary terms) in a column-oriented format, allowing fast queries with predicate pushdown. Parquet also supports various encoding and compression schemes natively, leading to dramatic reductions in file size.

However, the most critical part is that Parquet is widely supported in the data science ecosystem - supported out of the box by pandas, polars, and provided bindings for most programming languages. Parquet is also supported by a variety of databases and query engines (Athena, datafusion, Spark, BigQuery, etc), enabling direct SQL queries over petabyte-scale mass spectrometry data.

The format specified in this repository is a minimal set of columns needed to successfully process data - peak lists, isolation windows, and RT/ion mobility values. Each individual ion in an acquisition has it's own row in the mzparquet file (long format).

## Example of querying mzparquet files

```py
import polars as pl

df = pl.read_parquet("test.mzparquet")

# Find all MS2 ions with an m/z between (267.0, 267.1)
df.filter(
    pl.col("mz").is_between(267.0, 267.1)
  & pl.col("ms_level").eq(2)
)
```
