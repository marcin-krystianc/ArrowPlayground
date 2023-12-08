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

#define TO_FILE_ENDIANESS(x) (x)
#define FROM_FILE_ENDIANESS(x) (x)

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

/* File format:
--------------------------
| 0 - 3 | PQT1           | File header in ASCI
|------------------------|
| 4 - 7 | Row groups (n) | (uint32)
|------------------------|
| 8 - 11| Offset [0]     | (uint32)
|------------------------|
|12 - 15| Length [0]     | (uint32)
|------------------------|
|16 - 19| Offset [1]     | (uint32)
|------------------------|
|20 - 23| Length [1]     | (uint32)
|------------------------|

         .   .   .

|------------------------|
|.. - ..| Offset [n-1]   | (uint32)
|------------------------|
|.. - ..| Length [n-1]   | (uint32)
|------------------------|
|    Row group [0]       | Thrift data
|------------------------|
|    Row group [1]       | Thrift data
|------------------------|

         .   .   .

|------------------------|
|   Row group [n-1]      | Thrift data
--------------------------
*/
void GenerateRapidMetadata(const char *parquet_path, const char *index_file_path)
{
    char header_bytes[] = {'P', 'Q', 'T', '1'};

    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(parquet_path)));
    auto metadata = parquet::ReadMetaData(infile);

    std::ofstream fs(index_file_path, std::ios::out | std::ios::binary);
    fs.write(&header_bytes[0], sizeof(header_bytes));

    uint32_t row_groups = (metadata->num_row_groups());
    fs.write((char *)& TO_FILE_ENDIANESS(row_groups), sizeof(row_groups));

    auto offset0 = fs.tellp();

    // Write placeholders for offset and length
    for (uint32_t row_group = 0; row_group < row_groups; row_group++)
    {
        uint32_t zero = 0;
        fs.write((char *)& TO_FILE_ENDIANESS(zero), sizeof(zero)); 
        fs.write((char *)& TO_FILE_ENDIANESS(zero), sizeof(zero)); 
    }

    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;  

    // uint32_t base_offset = sizeof(header_bytes) + sizeof(row_groups) + row_groups * (sizeof(uint32_t) + sizeof(uint32_t));
    uint32_t offset = fs.tellp();
    for (uint32_t row_group = 0; row_group < row_groups; row_group++)
    {
        std::vector<int> rows_groups_subset = {(int)row_group};
        auto metadata_subset = metadata->Subset(rows_groups_subset);
        std::shared_ptr<arrow::io::BufferOutputStream> stream;
        PARQUET_ASSIGN_OR_THROW(stream, arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool()));
        metadata_subset.get()->WriteTo(stream.get());
        std::shared_ptr<arrow::Buffer> thrift_buffer;
        PARQUET_ASSIGN_OR_THROW(thrift_buffer, stream.get()->Finish());
        uint32_t length = thrift_buffer.get()->size();
        fs.write((const char*)thrift_buffer.get()->data(), length);
        offsets.push_back(offset);
        lengths.push_back(length);
        offset += length;
    }

    fs.seekp(offset0, std::ios_base::beg);
    for (uint32_t row_group = 0; row_group < row_groups; row_group++)
    {
        fs.write((char *)& TO_FILE_ENDIANESS(offsets[row_group]), sizeof(offsets[row_group])); 
        fs.write((char *)& TO_FILE_ENDIANESS(lengths[row_group]), sizeof(lengths[row_group])); 
    }

    fs.close(); 
}

std::vector<uint8_t> ReadRowGroupMetadata(const char *index_file_path, int row_group)
{
    // 1. Read metadata for a row group from the external file.
    std::ifstream fs(index_file_path, std::ios::binary);
    char header[4] = {};
    uint32_t row_groups;
    fs.read(&header[0], sizeof(header));
    fs.read((char*)&row_groups, sizeof(row_groups));
    row_groups = FROM_FILE_ENDIANESS(row_groups);
    if (row_group >= row_groups)
    {
        throw std::runtime_error("row_group > row_groups");
    }

    fs.seekg(2 * row_group * sizeof(uint32_t), std::ios_base::cur);

    uint32_t offset;    
    uint32_t length;
    fs.read((char*)&offset, sizeof(offset));
    offset = FROM_FILE_ENDIANESS(offset);
    fs.read((char*)&length, sizeof(length));
    length = FROM_FILE_ENDIANESS(length);
    
    std::vector<uint8_t> buffer (length);
    fs.seekg(offset, std::ios_base::beg);
    fs.read(&buffer[0], length);
    fs.close();
    return buffer;
}