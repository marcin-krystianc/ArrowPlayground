cdef extern from "rapid_parquet.h":
    void ReadMetadata(const char *filename);