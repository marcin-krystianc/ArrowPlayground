
import rapidparquet as rp
import pyarrow.parquet as pq
import polars as pl
import numpy as np

rows = 10
columns = 100
chunk_size = 1

path = "my.parquet"
table = pl.DataFrame(
    data=np.random.randn(rows, columns),
    schema=[f"c{i}" for i in range(columns)]).to_arrow()

pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False)
rapid_path = path + '.rapid'
rp.GenerateRapidMetadata(path, rapid_path)
metadata = rp.ReadRowGroupMetadata(rapid_path, 0)
print (metadata.num_row_groups)
{}
