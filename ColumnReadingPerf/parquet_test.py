import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import time
import polars as pl

t_read = []
t_write = []
t_read_100 = []
t_read_100_pre_buffer = []

ds = []
path = "/tmp/test_wide.parquet"

df = pl.DataFrame(
    data=np.random.randn(20_000, 10_000),
    schema=[f"c{i}" for i in range(10_000)]
)

for d in [100, 200, 300, 500, 1000, 2000, 10_000]:
    table = df.select([f"c{i}" for i in range(d)]).to_arrow()
    t = time.time()
    pq.write_table(table, path, row_group_size=100_000)
    t_write.append((time.time() - t)/d)

    t = time.time()
    res = pq.read_table(path)
    t_read.append((time.time() - t)/d)

    print(res.shape)

    t = time.time()
    res = pq.read_table(path, columns=[f"c{i}" for i in range(100)])
    t_read_100.append((time.time() - t)/100)
    ds.append(d)

plt.plot(ds, t_read_100, label='read 100 columns')
plt.plot(ds, t_read, label='read all columns')
plt.plot(ds, t_write, label='write')
plt.legend()
plt.show()