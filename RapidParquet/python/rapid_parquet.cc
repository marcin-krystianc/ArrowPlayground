#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

#include "rapid_parquet.h"

#include <iostream>
#include <fstream>

using arrow::Status;

int do_stuff()
{
    return 42;
}

void ReadMetadata(const char *filename)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(filename)));
    auto metadata = parquet::ReadMetaData(infile);

    std::cerr << "num row groups=" << metadata->num_row_groups() << std::endl;

    std::vector<int> rows_groups = {0};
    auto metadata_subset = metadata->Subset(rows_groups);
    
    std::cerr << "num row groups=" << metadata_subset->num_row_groups() << std::endl;
}