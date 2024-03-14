import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
from pyteomics.mzml import MzML
from timeit import default_timer
from collections import defaultdict

data = defaultdict(list)

for rep in range(5):
    start = default_timer()
    pl.read_parquet("/Users/michael/Downloads/b1906_293T_proteinID_01A_QE3_122212.mzparquet")
    elapsed = default_timer() - start
    data["long"].append(elapsed)

for rep in range(5):
    start = default_timer()
    pl.read_parquet("/Users/michael/Downloads/b1906_293T_proteinID_01A_QE3_122212.mzparquetW")
    elapsed = default_timer() - start
    data["wide"].append(elapsed)

for rep in range(5):
    start = default_timer()
    list(MzML("/Users/michael/Downloads/b1906_293T_proteinID_01A_QE3_122212.mzML"))
    elapsed = default_timer() - start
    data["mzML"].append(elapsed)

print(data)

df = pd.DataFrame(data)
sns.barplot(df)
plt.show()

