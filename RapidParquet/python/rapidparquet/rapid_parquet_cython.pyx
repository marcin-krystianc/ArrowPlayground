# distutils: sources = rapid_parquet.cc
# distutils: libraries = arrow parquet
# distutils: include_dirs = .
# distutils: language = c++

import pyarrow as pa
import pyarrow.parquet as pq
cimport rapidparquet.crapid_parquet
from pyarrow.lib cimport Buffer

def GenerateRapidMetadata(parquet_path, index_file_path):
    crapid_parquet.GenerateRapidMetadata(parquet_path.encode('utf8'), index_file_path.encode('utf8'))

def ReadRowGroupMetadata(index_file_path, row_group):
    v = crapid_parquet.ReadRowGroupMetadata(index_file_path.encode('utf8'), row_group)
    cdef char[::1] mv = <char[:v.size()]>&v[0]
    cdef Buffer pyarrow_buffer = pa.py_buffer(mv)
    return pq.core._parquet._reconstruct_filemetadata(pyarrow_buffer)
