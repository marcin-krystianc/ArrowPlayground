#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

void GenerateRapidMetadata(const char *parquet_path, const char *rapid_file_path);
std::vector<char> ReadRowGroupMetadata(const std::string& rapid_file_path, uint32_t row_group);
