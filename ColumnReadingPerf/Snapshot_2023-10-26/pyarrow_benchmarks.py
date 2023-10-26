import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import time
import csv
import gc

print (pq.__path__)


t_write = []

path = "/mnt/ramfs/my_ramfs.parquet"

columns_list = [100, 500, 5000]

chunks_list = [1, 2, 5, 10, 20, 50, 100, 200]
rows_list = [200_000]
data = []
repeats = 3

def writing_parameters (table, chunk_size, use_dictionary, write_statistics, compression, columns_to_read = 100):
    
    column_indices = [i for i in range(columns_to_read)]

    pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=use_dictionary, write_statistics=write_statistics, compression=compression)

    t_read = []
    t_read_p1 = []
    t_read_p2 = []

    for i in range(0, repeats):
        
        t = time.time()
        pr = pq.ParquetReader()
        pr.open(path)
        row_groups = [i for i in range(pr.num_row_groups)]
        p1 = time.time() - t

        t = time.time()
        res_data = pr.read_row_groups(row_groups, column_indices=column_indices, use_threads=False)
        p2 = time.time() - t

        t_read.append(p1 + p2)
        t_read_p1.append(p1)
        t_read_p2.append(p2)  

        del res_data
        gc.collect()

    return min(t_read) * 1_000_000, min(t_read_p1) * 1_000_000, min (t_read_p2) * 1_000_000

def read_with_external_schema (table, chunk_size, columns_to_read = 100):
              
    pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None)

    column_indices = [i for i in range(columns_to_read)]

    t_read = []
    t_read_p1 = []
    t_read_p2 = []

    for i in range(0, repeats):

        pr = pq.ParquetReader()
        pr.open(path)
        metadata = pr.metadata

        t = time.time()
        pr = pq.ParquetReader()
        pr.open(path, metadata=metadata)
        row_groups = [i for i in range(pr.num_row_groups)]
        p1 = time.time() - t

        t = time.time()
        res_data = pr.read_row_groups(row_groups, column_indices=column_indices, use_threads=False)
        p2 = time.time() - t

        t_read.append(p1 + p2)
        t_read_p1.append(p1)
        t_read_p2.append(p2)  

        del res_data
        gc.collect()

    return min(t_read) * 1_000_000, min(t_read_p1) * 1_000_000, min (t_read_p2) * 1_000_000

def make_table (nullable):
    return pa.Table.from_arrays(np.random.randn(columns, rows), schema=pa.schema([pa.field(f"c_{i}", pa.float64(), nullable=nullable) for i in range(columns)]))
    
for row_groups in chunks_list:
    for rows in rows_list:
        for columns in columns_list:
    
            chunk_size = rows / row_groups
            if chunk_size > int(chunk_size):
                chunk_size += 1
            
            chunk_size = int(chunk_size)
            print([chunk_size, rows, columns])

            ##################
            # NULLABLE
            ##################
            table = make_table(nullable=True)

            t, t1, t2 = read_with_external_schema(table=table, chunk_size=chunk_size)
            data.append(['read_with_external_metadata', columns, rows, row_groups, 100, t, t1, t2])

            t, t1, t2 = writing_parameters(table=table, chunk_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None)
            data.append(['fast_nullable', columns, rows, row_groups, 100, t, t1, t2])

            del table
            gc.collect()

with open('pyarrow_results.csv', 'w', encoding='UTF8', newline='') as f:

    writer = csv.writer(f)
        # write the header
    writer.writerow(['name','columns','rows','row_groups','columns_to_read','reading(μs)','reading_p1(μs)','reading_p2(μs)'])

    for dataRow in data:        
        writer.writerow(dataRow)
