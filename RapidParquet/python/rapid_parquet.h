#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

void ReadMetadata(const char *filename);
void GenerateRapidMetadata(const char *parquet_path, const char *index_file_path);
std::vector<char> ReadRowGroupMetadata(const char *index_file_path, int row_group);
