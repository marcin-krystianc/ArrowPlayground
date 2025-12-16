# Example
```
dotnet run -c Release --framework net6.0 --project /workspace/ArrowPlayground/PTPRepro/ -- test \ 
--batch-rows=6500000 \ 
--number-of-batches=23 \
--write-parquet=true \
--path=/tmp/my.parquet \
--columns=100 \
--max-row-group-length=10000000 \
--file-rows=100000000000 \
--exit-after=1000
```