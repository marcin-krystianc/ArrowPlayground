
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging;
using ParquetSharp;
using ParquetSharp.Arrow;
using Spectre.Console.Cli;

namespace PTPRepro;

public class Tester : AsyncCommand<TesterSettings>
{
    private const int NRandRows = 50_000_000;
        
    private static float[] RandomData = new float[NRandRows];
    private long n_written_rows_file = 0;
    private FileWriter _parquet_writer = null;
    private static readonly ILogger Log = LoggerFactory
        .Create(builder => builder.AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss ";
        }))
        .CreateLogger("Log");

    private void WriteRecordBatches(TesterSettings settings, Schema schema, IEnumerable<RecordBatch> recordBatches, bool useClones)
    {
        if (_parquet_writer == null || Interlocked.Read(ref n_written_rows_file) > settings.FileRows)
        {
            Interlocked.Exchange(ref n_written_rows_file, 0);
            _parquet_writer?.Close();
            // _parquet_writer?.Dispose();
            using var builder = new WriterPropertiesBuilder().MaxRowGroupLength(settings.MaxRowGroupLength).Compression(Compression.Snappy);
            using var b = builder.Build();
            _parquet_writer = new FileWriter(settings.Path, schema, b);
            _parquet_writer.NewBufferedRowGroup();
        }

        foreach (var recordBatch in recordBatches)
        {
            var recordBatchToWrite = useClones ? recordBatch.Clone() : recordBatch;
            if (settings.WriteParquet)
            {
                Interlocked.Add(ref n_written_rows_file, recordBatch.Length);
                _parquet_writer.WriteRecordBatch(recordBatchToWrite);
            }

            recordBatchToWrite.Dispose();
        }
    }

    private List<RecordBatch> GenerateBatches(TesterSettings settings, Schema schema)
    {
        var result = new List<RecordBatch>();
        var rnd = new Random();
        for (int i = 0; i < settings.NumberOfBatches; i++)
        {
            List<IArrowArray> data = new List<IArrowArray>();
            int rows = rnd.Next(Convert.ToInt32(0.99 * settings.BatchRows), settings.BatchRows + 1);
            for (var c = 0; c < settings.Columns; c++)
            {
                int startIndex = rnd.Next(0, RandomData.Length - settings.BatchRows + 1);
                Span<float> slice = new Span<float>(RandomData, startIndex, rows);

                var cBuilder = new FloatArray.Builder();
                foreach (var element in slice)
                {
                    cBuilder.Append(element);
                }

                FloatArray floatArray = cBuilder.Build();
                data.Add(floatArray);
            }

            // --- Combine arrays into a RecordBatch ---
            var recordBatch = new RecordBatch(schema, data: data, rows);
            result.Add(recordBatch);
        }

        return result;
    }

    public override async Task<int> ExecuteAsync(CommandContext context, TesterSettings settings)
    {
        var rnd = new Random();
        for (int i = 0; i < NRandRows; i++)
        {
            RandomData[i] = (float)rnd.NextDouble();
        }

        // Define the schema for the RecordBatch
        var fields = Enumerable.Range(0, settings.Columns)
            .Select(i => new Field($"C{i}", FloatType.Default, nullable: false))
            .ToList();

        var sw = Stopwatch.StartNew();
        var schema = new Schema(fields, []);
        var remainingBatches = new List<RecordBatch>();
        while (sw.Elapsed.TotalSeconds < settings.ExitAfter)
        {
            var batches = GenerateBatches(settings, schema)
                .Concat(remainingBatches)
                .ToList();

            remainingBatches = batches.OrderBy(_ => rnd.Next()) // random shuffle
                .Take(batches.Count / 2)
                .ToList();

            WriteRecordBatches(settings, schema, remainingBatches, true);
            WriteRecordBatches(settings, schema, batches.Except(remainingBatches), false);
        }

        Console.WriteLine("Hello, World!");
        await Task.Delay(0);
        return 0;
    }
}
