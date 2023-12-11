# distutils: sources = rapid_parquet.cc
# distutils: libraries = arrow parquet
# distutils: include_dirs = .
# distutils: language = c++
# cython: profile=False
# cython: language_level = 3

import pyarrow as pa
import pyarrow.parquet as pq

cimport cpython as cp
from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport Buffer

# from cython.cimports import crapid_parquet
cimport crapid_parquet

# Declare the function signature with cdef for Cython optimization
cdef int calculate_factorial(int n):
    if n == 0:
        return 1
    else:
        return n * calculate_factorial(n - 1)

# Define a Python accessible function
def factorial(int number):
    if number < 0:
        raise ValueError("Factorial is not defined for negative numbers")
    return calculate_factorial(number)

cdef void CReadMetadata(const char* path):
    crapid_parquet.ReadMetadata(path)

def ReadMetadata(filename):
    CReadMetadata(filename.encode('utf8'))

def GenerateRapidMetadata(parquet_path, index_file_path):
    crapid_parquet.GenerateRapidMetadata(parquet_path.encode('utf8'), index_file_path.encode('utf8'))

def ReadRowGroupMetadata(index_file_path, row_group):
    # buf = pa.allocate_buffer(0, resizable=True)
    # cdef:
    #     shared_ptr[CBuffer] c_buf = pa.pyarrow_unwrap_buffer(buf)
    v = crapid_parquet.ReadRowGroupMetadata(index_file_path.encode('utf8'), row_group)
    cdef char[::1] mv = <char[:v.size()]>&v[0]
    cdef Buffer pyarrow_buffer = pa.py_buffer(mv)
    return pq.core._parquet._reconstruct_filemetadata(pyarrow_buffer)
