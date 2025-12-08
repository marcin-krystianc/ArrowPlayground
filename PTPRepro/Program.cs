// See https://aka.ms/new-console-template for more information

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;
using CommunityToolkit.HighPerformance;
using ParquetSharp;
using ParquetSharp.Arrow;

namespace PTPRepro
{
    internal static class Program
    {
        private const int NColumns = 100;
        private const int NRandRows = 50_000_000;
        
        private static float[] RandomData = new float[NRandRows];
        
        public static int Main(string[] args)
        {
            var path = "tmp.parquet";
            
            var rnd = new Random();
            for (int i = 0; i < NRandRows; i++)
            {
                RandomData[i] = (float)rnd.NextDouble();
            }
            
            // Define the schema for the RecordBatch
            var fields = Enumerable.Range(0, NColumns)
                .Select(i => new Field($"C{i}", FloatType.Default, nullable: false))
                .ToList();

            var schema = new Schema(fields, Enumerable.Empty<KeyValuePair<string, string>>());
            var chunkSize = 2_000_000;
            using var builder = new WriterPropertiesBuilder().MaxRowGroupLength(chunkSize / 2).Compression(Compression.Snappy);
            using var b = builder.Build();
            using var parquetWriter = new FileWriter(path, schema, b);

            for (int i = 0; i < 10; i++)
            {
                List<IArrowArray> data = new List<IArrowArray>();

                for (var c = 0; c < NColumns; c++)
                {
                    int startIndex = rnd.Next(0, RandomData.Length - chunkSize + 1);
                    Span<float> slice = new Span<float>(RandomData, startIndex, chunkSize);

                    var cBuilder = new FloatArray.Builder();
                    foreach (var element in slice)
                    {
                        cBuilder.Append(element);
                    }

                    FloatArray floatArray = cBuilder.Build();
                    data.Add(floatArray);
                }

                // --- Combine arrays into a RecordBatch ---
                RecordBatch recordBatch = new RecordBatch(schema, data: data, chunkSize);
                parquetWriter.NewBufferedRowGroup();
                parquetWriter.WriteRecordBatch(recordBatch);
            }
            
            Console.WriteLine("Hello, World!");
            return 0;
        }
    }
}


