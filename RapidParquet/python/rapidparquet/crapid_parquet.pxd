
from libcpp.vector cimport vector

cdef extern from "rapid_parquet.h":
    cdef void ReadMetadata(const char *filename)
    cdef void GenerateRapidMetadata(const char *parquet_path, const char *index_file_path)
    cdef vector[char] ReadRowGroupMetadata(const char *index_file_path, int row_group)
