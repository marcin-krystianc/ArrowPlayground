using System.ComponentModel;
using Spectre.Console.Cli;

namespace PTPRepro;

public class TesterSettings : CommandSettings
{
    [CommandOption("--path")]
    [Description("Path where to write parquet files")]
    [DefaultValue("myparquet.path")]
    public string Path { get; set; }

    [CommandOption("--write-parquet")]
    [Description("Write arrow record batches to parquet files")]
    [DefaultValue(true)]
    public bool WriteParquet { get; set; }

    [CommandOption("--columns")]
    [Description("Number of columns")]
    [DefaultValue((int)100)]
    public int Columns { get; set; }
    
    [CommandOption("--max-row-group-length")]
    [Description("Number of rows in a row group")]
    [DefaultValue((long)10_000_000)]
    public long MaxRowGroupLength { get; set; }
    
    [CommandOption("--batch-rows")]
    [Description("Number of rows in a batch")]
    [DefaultValue((int)10_000_000)]
    public int BatchRows { get; set; }
    
    [CommandOption("--number-of-batches")]
    [Description("Number of batches")]
    [DefaultValue((long)1)]
    public long NumberOfBatches { get; set; }

    [CommandOption("--file-rows")]
    [Description("Number of rows in a file")]
    [DefaultValue((long)1_00_000_000_000)]
    public long FileRows { get; set; }

    [CommandOption("--exit-after")]
    [Description("Exit after this number of seconds")]
    [DefaultValue((long)1_000)]
    public long ExitAfter { get; set; }
}