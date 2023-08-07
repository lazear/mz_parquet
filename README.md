# mz_parquet

This repository houses a proof-of-concept for a modern, data lake-ready storage format for mass spectrometry data (mzparquet). A rust application is also provided for converting existing mzML files to mzparquet

Current standards like mzML, mzMLb, or mz5 have good characteristics for long-term data archival, but all require custom parsers to access data.

The Apache Parquet file format has emerged as the default file format for data science/big data workflows. Parquet allows storing nested data (such as lists of m/z and intensities, or key-value pairs of controlled vocabulary terms) in a column-oriented format, allowing fast queries with predicate pushdown. Parquet also supports various encoding and compression schemes natively, leading to dramatic reductions in file size.

However, the most critical part is that Parquet is widely supported in the data science ecosystem - supported out of the box by pandas, polars, and provided bindings for most programming languages. Parquet is also supported by a variety of databases and query engines (Athena, datafusion, Spark, BigQuery, etc), enabling direct SQL queries over petabyte-scale mass spectrometry data.

The proposed file format stores the most important data for analyzing MS data in individual columns (scan identifiers, ms_level, precursor list, scan_start_time, m/z, intensity, ion mobility, etc), and has a final column for storing lists of key-value pairs (cvParams in the mzML spec).


Prior work: https://github.com/compomics/ThermoRawFileParser - this would probably be the best way to go to mzparquet!

## Example of querying mzparquet files

```py
import polars as pl

df = pl.read_parquet("test.mzparquet")

# Find all MS2 scans containing an ion with a m/z between (267.0, 267.1)
df.filter(
    (
        pl.col("mz")
        .list.eval(pl.element().is_between(267.0, 267.1))
        .list.any()
    )
    & (pl.col("ms_level") == 2)
)


df.filter(pl.col("precursors").list.eval(pl.element().struct.field("selected_ion_mz").is_between(534, 535)).list.any()).head()
```


| id                                         |   ms_level | centroid   |   scan_start_time |   inverse_ion_mobility |   ion_injection_time |   total_ion_current | precursors   | mz                                                                      | intensity   | cv_params                                                                            |
|--------------------------------------------|------------|------------|-------------------|------------------------|----------------------|---------------------|--------------|-------------------------------------------------------------------------|-------------|--------------------------------------------------------------------------------------|
| controllerType=0 controllerNumber=1 scan=1 |          1 | True       |            15.002 |                    nan |               68.864 |         9.48546e+06 |              | [ 301.13785  301.14514  301.93048 ... 1339.5123  1339.9305  1362.5773 ] | [  930.3335  10505.5625    806.79913 ...  7536.851    7821.869
  7249.768  ]             | [{'accession': 'MS:1000133', 'value': 'controllerType=0 controllerNumber=1 scan=1'}] |


# Proposed schema
```
message schema {
    required byte_array id (utf8);
    required int32 ms_level;
    required boolean centroid;
    required float scan_start_time;
    optional float inverse_ion_mobility;
    required float ion_injection_time;
    required float total_ion_current;
    optional group precursors (list) {
        repeated group list {
            required group element {
                required float selected_ion_mz;
                optional int32 selected_ion_charge;
                optional float selected_ion_intensity;
                optional float isolation_window_target;
                optional float isolation_window_lower;
                optional float isolation_window_upper;
                optional byte_array spectrum_ref (utf8);
            }
        }
    }
    required group mz (LIST) {
        repeated group list {
            required float element;
        }
    }
    required group intensity (LIST) {
        repeated group list {
            required float element;
        }
    }
    optional group cv_params (list) {
        repeated group list {
            required group element {
                required byte_array accession (utf8);
                required byte_array value (utf8);
            }
        }
    }
}
```