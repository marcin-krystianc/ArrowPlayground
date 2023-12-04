import pyarrow.parquet as pq

def my_add(x, y):
    return x + y

def my_metadata(path):
    pr = pq.ParquetReader()
    pr.open(path)
    return pr.metadata