# ctypes_test.py
import ctypes
import pathlib
import rapidparquet as rp
import pyarrow.parquet as pq
import pyarrow.lib as pqlib
import pyarrow as pa
import polars as pl
import numpy as np

if __name__ == "__main__":
    # Load the shared library into ctypes
    libname = pathlib.Path().absolute() / "build/librapid_parquet.so"
    c_lib = ctypes.CDLL(libname)

# TODO (marcink) allocate_buffer
# buf = pa.allocate_buffer(5, resizable=True)


# buffer = pqlib.Buffer('aaa')
# pq.core._parquet._reconstruct_filemetadata('fasdf')

# result = c_lib.rapid_parquet.do_stuff()
print ("hello world:")
result = rp.my_add(2,3)
metada = rp.my_metadata('/workspace/tmp/my.parquet')
result = rp.factorial(5)

rows = 10
columns = 100
chunk_size = 1

path = "my.parquet"
table = pl.DataFrame(
    data=np.random.randn(rows, columns),
    schema=[f"c{i}" for i in range(columns)]).to_arrow()

pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False)
rp.ReadMetadata(path)
rapid_path = path + '.rapid'
rp.GenerateRapidMetadata(path, rapid_path)
metadata = rp.ReadRowGroupMetadata(rapid_path, 0)
print (metadata.row_groups)
{}