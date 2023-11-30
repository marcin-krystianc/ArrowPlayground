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
                const string pathSharp = @"d:\tmp\my_sharp.parquet";
                const string pathNet = @"d:\tmp\my_net.parquet";
                var data = GetData().ToArray();
                // await SaveToParquet2Sharp(data, pathSharp);
                await SaveToParquet2Net(data, pathNet);

                var columnsList = new[]
                {
                    1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
                    10000, 11000, 12000, 13000, 14000, 15000
                };

                var rowsList = new[] { 5000, };

                foreach (var rows in rowsList)
                foreach (var columns in columnsList)
                {
                    await WriteData(pathNet, columns, rows);
                    var elapsedMicroseconds = new List<long>();
                    for (int i = 0; i < 3; i++)
                    {
                        var sw = Stopwatch.StartNew();
                        await ReadData(pathNet);
                        sw.Stop();

                        elapsedMicroseconds.Add((long)sw.Elapsed.TotalMicroseconds / 100);
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

                using (Stream fs = System.IO.File.OpenWrite(path))
                {
                    using (ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs))
                    {
                        writer.CompressionMethod = CompressionMethod.None;
                        
                        using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                        {
                            Console.WriteLine("data");
                            var dataColumns = Enumerable.Range(0, columns)
                                .Select(i => new DataColumn(schema.DataFields[i],
                                    Enumerable.Range(0, rows).Select(_ => rnd.NextDouble()).ToArray()))
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
                using (Stream fs = System.IO.File.OpenRead(path))
                {
                    using (ParquetReader reader = await ParquetReader.CreateAsync(fs))
                    {
                        for (int i = 0; i < reader.RowGroupCount; i++)
                        {
                            using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i))
                            {
                                foreach (DataField df in reader.Schema.GetDataFields().Take(100))
                                {
                                    DataColumn columnData = await rowGroupReader.ReadColumnAsync(df);
                                    if (columnData.Data == null) throw new Exception("aaaa");

                                    // do something to the column...
                                }
                            }
                        }
                    }
                }
            }

            public static IEnumerable<SingleCandlestickMP> GetData()
            {
                var maxDelta = 10;
                var rnd = new Random(0);
                SingleCandlestickMP prev = null;
                SingleCandlestickMP current;
                for (var i = 0; i < 2673685; i++)
                {
                    if (prev is null)
                    {
                        current = new SingleCandlestickMP
                        {
                            Id = rnd.NextInt64(),
                            OpenTime = rnd.NextInt64(),
                            Open = rnd.NextInt64(),
                            High = rnd.NextInt64(),
                            Low = rnd.NextInt64(),
                            Close = rnd.NextInt64(),
                            Volume = rnd.NextInt64(),
                            CloseTime = rnd.NextInt64(),
                        };
                    }
                    else
                    {
                        current = new SingleCandlestickMP
                        {
                            Id = prev.Id + rnd.Next(maxDelta),
                            OpenTime = prev.OpenTime + rnd.Next(maxDelta),
                            Open = prev.Open + rnd.Next(maxDelta),
                            High = prev.High + rnd.Next(maxDelta),
                            Low = prev.Low + rnd.Next(maxDelta),
                            Close = prev.Close + rnd.Next(maxDelta),
                            Volume = rnd.NextInt64(),
                            CloseTime = prev.CloseTime + rnd.Next(maxDelta),
                        };
                    }

                    prev = current;
                    yield return current;
                }
            }

            public static async Task SaveToParquet2Sharp(IEnumerable<SingleCandlestickMP> data, string fileFullPath)
            {
                var columns = new ParquetSharp.Column[]
                {
                    new ParquetSharp.Column<long>("Id"),
                    new ParquetSharp.Column<long>("OpenTime"),
                    new ParquetSharp.Column<long>("Open"),
                    new ParquetSharp.Column<long>("High"),
                    new ParquetSharp.Column<long>("Low"),
                    new ParquetSharp.Column<long>("Close"),
                    new ParquetSharp.Column<long>("Volume"),
                    new ParquetSharp.Column<long>("CloseTime")
                };

                var rows = data.ToArray();
                var builder = new ParquetSharp.WriterPropertiesBuilder();
                builder.DisableDictionary();
                builder.Compression(ParquetSharp.Compression.Snappy);
                //builder.Compression(Compression.Zstd);
                builder.Encoding(ParquetSharp.Encoding.DeltaBinaryPacked);
                builder.MaxRowGroupLength(10_000_000);
                builder.Version(ParquetSharp.ParquetVersion.PARQUET_1_0);
                builder.DataPagesize(1_000_000_000);
                builder.DisableWritePageIndex();
                builder.DisableStatistics();
                var writerProperties = builder.Build();

                using var writer = ParquetSharp.RowOriented.ParquetFile.CreateRowWriter<SingleCandlestickMP>(fileFullPath,
                        writerProperties, columns);
                writer.WriteRows(rows);
                writer.Close();
            }
            
            public static async Task SaveToParquet2Net(IEnumerable<SingleCandlestickMP> data, string path)
            {
                var options = new Parquet.Serialization.ParquetSerializerOptions
                {
                    CompressionMethod = Parquet.CompressionMethod.Snappy, // Zstd
                    RowGroupSize = 10_000_000,
                    ParquetOptions = new ParquetOptions()
                    {
                        UseDictionaryEncoding = false,
                        UseDeltaBinaryPackedEncoding = true,
                    }
                };
                    
                await Parquet.Serialization.ParquetSerializer.SerializeAsync(data, path, options);
            }
        }
    }
}