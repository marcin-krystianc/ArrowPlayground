#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

#include "rapid_parquet.h"

int do_stuff()
{
    return 42;
}

parquet::FileMetaData ReadMetadata(const char *filename)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    auto metadata = parquet::ReadMetaData(infile);
   
    std::vector<int> rows_groups = {0};
    auto metadata_subset = metadata->Subset(rows_groups);
    
    return metadata;
}
