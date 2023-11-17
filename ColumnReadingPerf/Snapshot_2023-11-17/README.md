## Arrow changes
- skip_columns -> https://github.com/marcin-krystianc/arrow/commits/skip_columns
- skip_rows -> https://github.com/marcin-krystianc/arrow/commits/skip_rows
- skip_rows_and_columns -> https://github.com/marcin-krystianc/arrow/commits/skip_rows_and_columns

## Conclusions
- Thrift format requires reading all bytes sequentially so it is not possible to skip reading and decoding all bytes in the buffer
- It is possible though to skip instantiating objects from the decoding data
- It is possible to improve the performance of Parquet metadata reading if we skip instantiating some rows or some columns, but the max speedup seems to be only 2x
<img width="1916" alt="Screenshot 2023-10-31 143748" src="https://github.com/marcin-krystianc/ArrowPlayground/blob/master/ColumnReadingPerf/Snapshot_2023-11-17/cpp_skip_row_and_columns.png">