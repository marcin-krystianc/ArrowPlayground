## Conclusions
- Pyarrow is slower in reading data than raw C++, the more rows in the row group there is the larger the performance difference

<img width="1916" alt="Screenshot 2023-10-31 143748" src="https://github.com/marcin-krystianc/ArrowPlayground/blob/master/ColumnReadingPerf/Snapshot_2023-10-31/python_vs_cpp_row_group_size.png">