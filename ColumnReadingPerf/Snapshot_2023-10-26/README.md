## Introduction
- This plot presents the effect of increasing the number of row groups in the Parquet file (single file without partitioning)
- The exact code to generate this plot is available on https://github.com/marcin-krystianc/ArrowPlayground/tree/master/ColumnReadingPerf/Snapshot_2023-10-26
- First row shows results for reading a file with 100 columns
- The second row shows results for reading a file with 500 columns
- The third row shows results for reading a file with 5000 columns
- The first column shows the amortized time needed to read one column when we read 100 columns from the file
- The second column shows the amortized time needed to "open the file" and read its metadata
- The third column shows the amortized time needed to read the actual data for the entire column (all row groups)

## Conclusions
- There is an optimal row group size, if the row group size is too large or too small the column reading performance is not optimal
- The more row groups there are in the file the longer it takes to open the file and read the metadata (blue line)
- When the metadata is passed to the ParquetReader, the more row groups there are in the file the larger the performance win is

<img width="1916" alt="Screenshot 2023-10-06 143748" src="https://github.com/marcin-krystianc/ArrowPlayground/blob/master/ColumnReadingPerf/Snapshot_2023-10-26/row_groups.png">