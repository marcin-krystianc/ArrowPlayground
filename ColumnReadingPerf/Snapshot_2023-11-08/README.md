## Conclusions
- Reading with external metadata is many times faster if the number of row groups is high
- Storing schema in the file makes it slower, but it matters only if the number of the row groups is low

<img width="1916" alt="Screenshot 2023-10-31 143748" src="https://github.com/marcin-krystianc/ArrowPlayground/blob/master/ColumnReadingPerf/Snapshot_2023-11-08/external_metadata_store_schema.png">