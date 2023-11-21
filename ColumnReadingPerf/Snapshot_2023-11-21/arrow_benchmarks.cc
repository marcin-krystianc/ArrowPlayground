#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

#include <iostream>
#include <list>
#include <chrono>
#include <random>
#include <vector>
#include <fstream>
#include <filesystem>
#include <iomanip>

using arrow::Status;

namespace
{

  class TheMetadata
  {
  public:
    int GetValue() { return 1; }
  };

  class MyMetadata : public TheMetadata
  {
  public:
    int GetValue() { return 2; }
  };

  const char *FILE_NAME = "/mnt/ramfs/my_ramfs.parquet";

  std::shared_ptr<arrow::Schema> GetSchema(size_t nColumns)
  {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // For simplicity, we'll create int32 columns. You can expand this to handle other types.
    for (int i = 0; i < nColumns; i++)
    {
      fields.push_back(arrow::field("c_" + std::to_string(i), arrow::float32(), false));
    }

    return arrow::schema(fields);
  }

  Status BuildMetadata(size_t nColumns, size_t nRows, size_t chunkSize, parquet::FileMetaData *out)
  {
    auto schema = GetSchema(nColumns);
    parquet::WriterProperties::Builder builder;
    auto writerProperties = builder
                                .max_row_group_length(chunkSize)
                                ->disable_dictionary()
                                ->disable_statistics()
                                ->build();

    parquet::ArrowWriterProperties::Builder arrowWriterPropertiesBuilder;
    auto arrowWriterProperties = arrowWriterPropertiesBuilder
                                     .set_use_threads(false)
                                     // .store_schema()
                                     ->build();

    std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;

    ARROW_RETURN_NOT_OK(parquet::arrow::ToParquetSchema(schema.get(), *writerProperties, *arrowWriterProperties, &schemaDescriptor));

    auto fileMetaDataBuilder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), writerProperties);
    auto rowOrdinal = 0;
    for (auto rows = nRows; rows > 0;)
    {
      auto rowsToWrite = rows > chunkSize ? chunkSize : rows;
      auto rowGroupBuilder = fileMetaDataBuilder->AppendRowGroup();

      // Ensures all columns have been written
      rowGroupBuilder->set_num_rows(rowsToWrite);
      rowGroupBuilder->Finish(0, rowOrdinal);
      rowOrdinal++;
      rows -= rowsToWrite;
    }

    fileMetaDataBuilder->Finish();
    return Status::OK();
  }

  std::shared_ptr<arrow::Table> GetTable(size_t nColumns, size_t nRows)
  {
    std::random_device dev;
    std::default_random_engine rng(dev());
    std::uniform_real_distribution<> rand_gen(0.0, 1.0);

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // For simplicity, we'll create int32 columns. You can expand this to handle other types.
    for (int i = 0; i < nColumns; i++)
    {
      arrow::FloatBuilder builder;
      for (auto j = 0; j < nRows; j++)
      {
        if (!builder.Append(rand_gen(rng)).ok())
          throw std::runtime_error("builder.Append");
      }

      std::shared_ptr<arrow::Array> array;
      if (!builder.Finish(&array).ok())
        throw std::runtime_error("builder.Finish");

      arrays.push_back(array);
      fields.push_back(arrow::field("c_" + std::to_string(i), arrow::float32(), false));
    }

    auto table = arrow::Table::Make(arrow::schema(fields), arrays);
    return table;
  }

  Status WriteTableToParquet(size_t nColumns, size_t nRows, const std::string &filename, std::chrono::microseconds *dt, int64_t chunkSize)
  {
    auto table = GetTable(nColumns, nRows);
    auto begin = std::chrono::steady_clock::now();
    auto result = arrow::io::FileOutputStream::Open(filename);
    auto outfile = result.ValueOrDie();
    parquet::WriterProperties::Builder builder;
    auto properties = builder
                          .max_row_group_length(chunkSize)
                          ->disable_dictionary()
                          ->disable_statistics()
                          ->build();

    parquet::ArrowWriterProperties::Builder arrowWriterPropertiesBuilder;
    auto arrowWriterProperties = arrowWriterPropertiesBuilder
                                     .set_use_threads(false)
                                     // .store_schema()
                                     ->build();

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, chunkSize, properties, arrowWriterProperties));
    auto end = std::chrono::steady_clock::now();
    *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return Status::OK();
  }

  Status ReadRwoGroupWithRowSubsetMetadata(const std::string &filename, int row_group, std::vector<int> column_indicies, std::chrono::microseconds *dt, std::chrono::microseconds *dt1, std::chrono::microseconds *dt2)
  {
    auto index_file_name = filename + " .index";

    // Prepare metadata for a particular row group.
    // This is not taken into account for measurement, because it is going to be done only once per parquet file.
    {
      std::shared_ptr<arrow::io::ReadableFile> infile;
      ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
      auto metadata = parquet::ReadMetaData(infile);

      std::vector<int> row_groups = {row_group};
      auto single_row_metadata_tmp = metadata.get()->Subset(row_groups);
      std::shared_ptr<arrow::io::BufferOutputStream> stream;
      ARROW_ASSIGN_OR_RAISE(stream, arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool()));
      single_row_metadata_tmp.get()->WriteTo(stream.get());
      std::shared_ptr<arrow::Buffer> thrift_buffer;
      ARROW_ASSIGN_OR_RAISE(thrift_buffer, stream.get()->Finish());

      std::ofstream index_file(index_file_name, std::ios::binary);
      index_file.write((const char*)thrift_buffer.get()->data(), thrift_buffer.get()->size());
      index_file.close();
    }

    ///////////////////////////////////////////////////////////////////////////
    // DT1 Begin
    ///////////////////////////////////////////////////////////////////////////
    std::unique_ptr<parquet::arrow::FileReader> reader;
    {
      auto begin = std::chrono::steady_clock::now();
  
      // 1. Read metadata for a row group from the external file.
      std::ifstream index_file(index_file_name, std::ios::binary);
      index_file.seekg(0, std::ios::end);
      size_t index_file_length = index_file.tellg();
      index_file.seekg(0, std::ios::beg);
      std::vector<char> buffer (index_file_length);
      index_file.read(&buffer[0], index_file_length);
      index_file.close();

      // 2. Deserialize the metadata
      uint32_t read_metadata_len = index_file_length;
      auto single_row_metadata = parquet::FileMetaData::Make(&buffer[0], &read_metadata_len);

      auto readerProperties = parquet::default_reader_properties();
      auto arrowReaderProperties = parquet::default_arrow_reader_properties();
      arrowReaderProperties.set_pre_buffer(true);

      parquet::arrow::FileReaderBuilder fileReaderBuilder;
      fileReaderBuilder.properties(arrowReaderProperties);

      // 3. Open the file using metadata for a row group  
      ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties, single_row_metadata));
      ARROW_ASSIGN_OR_RAISE(reader, fileReaderBuilder.Build());

      auto end = std::chrono::steady_clock::now();
      *dt1 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    }

    ///////////////////////////////////////////////////////////////////////////
    // DT1 End
    ///////////////////////////////////////////////////////////////////////////
   
    ///////////////////////////////////////////////////////////////////////////
    // DT2 Begin
    ///////////////////////////////////////////////////////////////////////////
    {
      auto begin = std::chrono::steady_clock::now();
      std::shared_ptr<arrow::Table> parquet_table;
      // Read the table.
      ARROW_RETURN_NOT_OK(reader->ReadRowGroup(0, column_indicies, &parquet_table));
      auto end = std::chrono::steady_clock::now();
      *dt2 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    }

    ///////////////////////////////////////////////////////////////////////////
    // DT2 End
    ///////////////////////////////////////////////////////////////////////////

    *dt = *dt1 + *dt2;    
    return Status::OK();
  }

  Status ReadColumnsAsTable(const std::string &filename, std::vector<int> indicies, std::chrono::microseconds *dt, std::chrono::microseconds *dt1, std::chrono::microseconds *dt2)
  {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));

    auto begin = std::chrono::steady_clock::now();
    auto readerProperties = parquet::default_reader_properties();
    auto arrowReaderProperties = parquet::default_arrow_reader_properties();
    arrowReaderProperties.set_pre_buffer(true);

    parquet::arrow::FileReaderBuilder fileReaderBuilder;
    fileReaderBuilder.properties(arrowReaderProperties);
    ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties));
    auto reader = fileReaderBuilder.Build();
    // reader->init();
    auto end = std::chrono::steady_clock::now();

    *dt1 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);

    begin = std::chrono::steady_clock::now();
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    ARROW_RETURN_NOT_OK(reader->get()->ReadRowGroup(0, indicies, &parquet_table));

    end = std::chrono::steady_clock::now();
    *dt2 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    *dt = *dt1 + *dt2;
    return Status::OK();
  }

  Status RunMain(int argc, char **argv)
  {
    std::ofstream csvFile;
    csvFile.open("arrow_results.csv", std::ios_base::out); // append instead of overwrite
    csvFile << "name,columns,rows,chunk_size,data_page_size,columns_to_read,reading(μs),reading_p1(μs),reading_p2(μs)" << std::endl;

    std::vector<int> nColumns = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
    std::vector<int> rows_list = {50000};
    std::vector<int64_t> chunk_sizes = {100, 1000, 50000};
    const int repeats = 10;

    std::vector<int> indicies(100);
    std::iota(indicies.begin(), indicies.end(), 0);

    for (auto chunk_size : chunk_sizes)
    {
      for (int nRow : rows_list)
      {
        for (int nColumn : nColumns)
        {
          std::chrono::microseconds writing_dt;

          std::cerr << " writing file" << std::endl;
          // if (!std::filesystem::exists(FILE_NAME))
          ARROW_RETURN_NOT_OK(WriteTableToParquet(nColumn, nRow, FILE_NAME, &writing_dt, chunk_size));
          std::cerr << " reading file" << std::endl;

          {
            std::vector<std::chrono::microseconds> reading_100_dts(repeats);
            std::vector<std::chrono::microseconds> reading_100_dts1(repeats);
            std::vector<std::chrono::microseconds> reading_100_dts2(repeats);
            for (int i = 0; i < repeats; i++)
            {
              ARROW_RETURN_NOT_OK(ReadColumnsAsTable(FILE_NAME, indicies, &reading_100_dts[i], &reading_100_dts1[i], &reading_100_dts2[i]));
            }

            auto reading_100_dt = *std::min_element(reading_100_dts.begin(), reading_100_dts.end());
            auto reading_100_dt1 = *std::min_element(reading_100_dts1.begin(), reading_100_dts1.end());
            auto reading_100_dt2 = *std::min_element(reading_100_dts2.begin(), reading_100_dts2.end());

            csvFile << "cpp"
                    << ","
                    << nColumn << ","
                    << nRow << ","
                    << chunk_size << ","
                    << 1024 * 1024 * 1024 << ","
                    << 100 << ","
                    << reading_100_dt.count() << ","
                    << reading_100_dt1.count() << ","
                    << reading_100_dt2.count()
                    << std::endl;
          }

          {
            std::vector<std::chrono::microseconds> reading_100_dts(repeats);
            std::vector<std::chrono::microseconds> reading_100_dts1(repeats);
            std::vector<std::chrono::microseconds> reading_100_dts2(repeats);
            for (int i = 0; i < repeats; i++)
            {
              ARROW_RETURN_NOT_OK(ReadRwoGroupWithRowSubsetMetadata(FILE_NAME, 0, indicies, &reading_100_dts[i], &reading_100_dts1[i], &reading_100_dts2[i]));
            }

            auto reading_100_dt = *std::min_element(reading_100_dts.begin(), reading_100_dts.end());
            auto reading_100_dt1 = *std::min_element(reading_100_dts1.begin(), reading_100_dts1.end());
            auto reading_100_dt2 = *std::min_element(reading_100_dts2.begin(), reading_100_dts2.end());

            csvFile << "cpp_metadata_subset"
                    << ","
                    << nColumn << ","
                    << nRow << ","
                    << chunk_size << ","
                    << 1024 * 1024 * 1024 << ","
                    << 100 << ","
                    << reading_100_dt.count() << ","
                    << reading_100_dt1.count() << ","
                    << reading_100_dt2.count()
                    << std::endl;
          }
        }
      }
    }

    return Status::OK();
  }
} // namespace

int main(int argc, char **argv)
{
  std::chrono::microseconds writing_dt;

  Status st = RunMain(argc, argv);
  if (!st.ok())
  {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}