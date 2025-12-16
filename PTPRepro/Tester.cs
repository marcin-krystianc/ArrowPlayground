
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
    private readonly IArrowType[] COLUMN_TYPES = { FloatType.Default, StringType.Default, HalfFloatType.Default, };
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
            for (var c = 0; c < schema.FieldsList.Count; c++)
            { 
                var dataType = schema.FieldsList[c].DataType;
                if (dataType is FloatType)
                {
                    int startIndex = rnd.Next(0, RandomData.Length - settings.BatchRows + 1);
                    Span<float> slice = new Span<float>(RandomData, startIndex, rows);

                    var cBuilder = new FloatArray.Builder();
                    foreach (var element in slice)
                    {
                        cBuilder.Append(element);
                    }

                    data.Add(cBuilder.Build());
                }
                else if (dataType is StringType)
                {
                    int startIndex = rnd.Next(0, RandomData.Length - settings.BatchRows + 1);
                    Span<float> slice = new Span<float>(RandomData, startIndex, rows);

                    var cBuilder = new StringArray.Builder();
                    foreach (var element in slice)
                    {
                        cBuilder.Append(element.ToString(CultureInfo.InvariantCulture));
                    }

                    data.Add(cBuilder.Build());
                }
                else if (dataType is HalfFloatType)
                {
                    int startIndex = rnd.Next(0, RandomData.Length - settings.BatchRows + 1);
                    Span<float> slice = new Span<float>(RandomData, startIndex, rows);

                    var cBuilder = new HalfFloatArray.Builder();
                    foreach (var element in slice)
                    {
                        cBuilder.Append((Half)element);
                    }

                    data.Add(cBuilder.Build());
                }
                else
                {
                    throw new Exception($"Unsupported data type: {dataType}");
                }
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
            .Select(i => new Field($"C{i}", COLUMN_TYPES[i % COLUMN_TYPES.Length], nullable: false))
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
