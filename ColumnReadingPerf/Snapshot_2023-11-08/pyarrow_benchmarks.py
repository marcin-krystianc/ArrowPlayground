import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import time
import csv
import gc

print (pq.__path__)


t_write = []

path = "/mnt/ramfs/my_ramfs.parquet"
base_dir = "/mnt/ramfs/"

#columns_list = [1000]
#chunks_list = [1]
#rows_list = [1000]
#repeats = 30000
columns_list = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
chunks_list = [1_000, 50_000]
rows_list = [50_000]
data = []
repeats = 3

data_page_size = 1024 * 1024 * 1024

def read_with_parquet_reader (columns_to_read = 100):
    
    column_indices = [i for i in range(columns_to_read)]

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
        res_data = pr.read_row_groups(row_groups, column_indices=column_indices, use_threads=False,)
        p2 = time.time() - t

        t_read.append(p1 + p2)
        t_read_p1.append(p1)
        t_read_p2.append(p2)  

        del res_data
        gc.collect()

    return min(t_read) * 1_000_000, min(t_read_p1) * 1_000_000, min (t_read_p2) * 1_000_000

def read_with_external_schema (columns_to_read = 100,):

    column_indices = [i for i in range(columns_to_read)]

    t_read = []
    t_read_p1 = []
    t_read_p2 = []
 
    pr = pq.ParquetReader()
    pr.open(path)
    metadata = pr.metadata
    
    for i in range(0, repeats):
       

        t = time.time()
        pr = pq.ParquetReader()
        pr.open(path, metadata=metadata)

        #pr.open(path)
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
    return pa.Table.from_arrays(np.random.rand(columns, rows), schema=pa.schema([pa.field(f"c_{i}", pa.float32(), nullable=nullable) for i in range(columns)]))
    

for chunk_size in chunks_list:
    for rows in rows_list:
        for columns in columns_list:
               
            print([chunk_size, rows, columns])

            ##################
            # NULLABLE
            ##################
            table = make_table(nullable=False)
            
            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None, data_page_size = data_page_size, store_schema = True)
            t, t1, t2 = read_with_external_schema()
            data.append(['read_external_metadata_store_true', columns, rows, chunk_size, data_page_size, 100, t, t1, t2])
            
            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None, data_page_size = data_page_size, store_schema = False)
            t, t1, t2 = read_with_external_schema()
            data.append(['read_external_metadata_store_false', columns, rows, chunk_size, data_page_size, 100, t, t1, t2])

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None, data_page_size = data_page_size, store_schema = True)
            t, t1, t2 = read_with_parquet_reader()
            data.append(['read_store_true', columns, rows, chunk_size, data_page_size, 100, t, t1, t2])
            
            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None, data_page_size = data_page_size, store_schema = False)
            t, t1, t2 = read_with_parquet_reader()
            data.append(['read_store_false', columns, rows, chunk_size, data_page_size, 100, t, t1, t2])

            #ds.write_dataset(table, base_dir, format="parquet", partitioning=ds.partitioning(pa.schema([("year", pa.int16())])))
            
            del table
            gc.collect()

with open('pyarrow_results.csv', 'w', encoding='UTF8', newline='') as f:

    writer = csv.writer(f)
        # write the header
    writer.writerow(['name','columns','rows','chunk_size','data_page_size', 'columns_to_read','reading(μs)','reading_p1(μs)','reading_p2(μs)'])

    for dataRow in data:        
        writer.writerow(dataRow)
