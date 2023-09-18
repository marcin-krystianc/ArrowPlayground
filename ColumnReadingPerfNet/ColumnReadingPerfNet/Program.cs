
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace ColumnReadingPerfNet
{
    internal static class ColumnReadingPerfNet
    {
        class Program
        {
            static async Task Main(string[] args)
            {
                var path = @"d:\tmp\my_net.parquet";
                
                var columnsList = new[]{
                    100, 200, 300, 400, 500, 600, 700, 800, 900,
                    1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
                    10000, 20000, 30000, 40000, 
                    50000
                };

                var rowsList = new[] { 5000, };
                
                foreach(var rows in rowsList)
                foreach (var columns in columnsList)
                {
                    await WriteData(path, columns, rows);
                    var elapsedMicroseconds = new List<long>();
                    for(int i = 0; i < 3;i++)
                    {
                        var sw = Stopwatch.StartNew();
                        await ReadData(path);
                        sw.Stop();
                        
                        elapsedMicroseconds.Add( (long)sw.Elapsed.TotalMicroseconds / 100);
                    }

                    Console.WriteLine($"Columns={columns}, rows={rows}, Reading_100={elapsedMicroseconds.Min()}");
                }
            }

            static async Task WriteData(string path, int columns, int rows)
            {
                var rnd = new Random(0);
                
                var fields = Enumerable.Range(0, columns)
                    .Select(x => new DataField<double>($"c_{x}"));
                var schema = new ParquetSchema(fields);
                    
                using(Stream fs = System.IO.File.OpenWrite(path)) 
                {
                    using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs))
                    {
                        writer.CompressionMethod = CompressionMethod.Gzip;
                        
                        using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                        {
                            Console.WriteLine("data");
                            var dataColumns = Enumerable.Range(0, columns)
                                .Select(i => new DataColumn(schema.DataFields[i], Enumerable.Range(0, rows).Select(_ => rnd.NextDouble()).ToArray()))
                                .ToArray();
                                
                            Console.WriteLine("WriteColumnAsync");
                            foreach (var dataColumn in dataColumns)
                            {
                                await groupWriter.WriteColumnAsync(dataColumn);
                            }
                        }
                    }
                }
            }

            static async Task ReadData(string path)
            {
                using(Stream fs = System.IO.File.OpenRead(path))
                {
                    using(ParquetReader reader = await ParquetReader.CreateAsync(fs))
                    {
                        for(int i = 0; i < reader.RowGroupCount; i++)
                        {
                            using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i)) {

                                foreach(DataField df in reader.Schema.GetDataFields().Take(100)) {
                                    DataColumn columnData = await rowGroupReader.ReadColumnAsync(df);
                                    if (columnData.Data == null) throw new Exception("aaaa");
                                
                                    // do something to the column...
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}