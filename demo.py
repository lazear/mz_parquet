import polars as pl

df = pl.read_parquet("test.mzParquet")

# Find all MS2 scans containing an ion with a m/z between (267.0, 267.1)
df.filter(
    (
        pl.col("mz")
        .list.eval(pl.element().is_between(267.0, 267.1))
        .list.any()
    )
    & (pl.col("ms_level") == 2)
)

# Find all MS2 scans where any precursor has an m/z betwen 534, 535
df.filter(pl.col("precursors").list.eval(pl.element().struct.field("selected_ion_mz").is_between(534, 535)).list.any())