#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

int do_stuff();

parquet::FileMetaData ReadMetadata(const char *filename);
