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
#include <unistd.h>

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
  // const char *FILE_NAME = "/tmp/my.parquet";

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

  Status ReadColumnsAsTableExternalMetadata(const std::string &filename, std::vector<int> indicies, std::chrono::microseconds *dt, std::chrono::microseconds *dt1, std::chrono::microseconds *dt2)
  {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    auto metadata = parquet::ReadMetaData(infile);

    auto begin = std::chrono::steady_clock::now();
    auto readerProperties = parquet::default_reader_properties();
    auto arrowReaderProperties = parquet::default_arrow_reader_properties();
    arrowReaderProperties.set_pre_buffer(true);

    parquet::arrow::FileReaderBuilder fileReaderBuilder;
    fileReaderBuilder.properties(arrowReaderProperties);
    ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties, metadata));
    auto reader = fileReaderBuilder.Build();
    // reader->init();
    auto end = std::chrono::steady_clock::now();

    *dt1 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);

    begin = std::chrono::steady_clock::now();
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    ARROW_RETURN_NOT_OK(reader->get()->ReadTable(indicies, &parquet_table));

    end = std::chrono::steady_clock::now();
    *dt2 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    *dt = *dt1 + *dt2;
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

  void ReadFileContent(const std::string &filename)
  {
    std::list<void *> list;
    for (int i = 0;; i++)
    {
      std::ifstream infile(filename, std::ios_base::binary);
      infile.seekg(0, std::ios::end);
      size_t length = infile.tellg();
      infile.seekg(0, std::ios::beg);
      auto buffer = new char[length];
      infile.read(buffer, length);
      list.push_back(buffer);
      std::cerr << i << std::endl;
    }
  }

  Status ReadMetadata(const std::string &filename)
  {
    std::list<std::shared_ptr<parquet::FileMetaData>> list;
    for (int i = 0; i < 10000; i++)
    {
      std::shared_ptr<arrow::io::ReadableFile> infile;
      ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
      auto metadata = parquet::ReadMetaData(infile);
      std::cerr << "Metadata columns: " << metadata->num_columns() << " rows: " << metadata->num_rows() << " row_groups: " << metadata->num_row_groups() << std::endl;

      std::vector<int> rows_groups = {0};
      auto metadata_subset = metadata->Subset(rows_groups);
      std::cerr << "metadata_subset columns: " << metadata_subset->num_columns() << " rows: " << metadata_subset->num_rows() << " row_groups: " << metadata_subset->num_row_groups() << std::endl;

      // auto impl = metadata->impl_.get();
      // auto format = metadata->GetFormat();

      // list.push_back(metadata);
      std::cerr << i << std::endl;
    }

    return Status::OK();
  }

  Status RunMain(int argc, char **argv)
  {
    std::ofstream csvFile;
    csvFile.open("arrow_results.csv", std::ios_base::out); // append instead of overwrite
    csvFile << "name,columns,rows,chunk_size,data_page_size,columns_to_read,reading(μs),reading_p1(μs),reading_p2(μs)" << std::endl;

    // std::vector<int> nColumns = {10000};
    // std::vector<int> rows_list = {10};
    // std::vector<int64_t> chunk_sizes = {1};
    std::vector<int> nColumns = {1000, 2000};
    std::vector<int> rows_list = {50000};
    std::vector<int64_t> chunk_sizes = {1000, 50000};
    const int repeats = 10;
    // const int repeats = 3000;

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

            std::cerr << "(" << nColumn << ", " << nRow << ")"
                      << ", chunk_size=" << chunk_size
                      << ", writing_dt=" << writing_dt.count() / nColumn
                      << ", reading_100_dt=" << reading_100_dt.count()
                      // << ", reading_100_dt=" << reading_100_dt.count() / 100
                      // << ", reading_100_dt1=" << reading_100_dt1.count() / 100
                      << ", reading_100_dt1=" << reading_100_dt1.count()
                      // << ", reading_100_dt2=" << reading_100_dt2.count() / 100
                      << ", reading_100_dt2=" << reading_100_dt2.count()
                      << std::endl;

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

            std::cerr << "(" << nColumn << ", " << nRow << ")"
                      << ", chunk_size=" << chunk_size
                      << ", writing_dt=" << writing_dt.count() / nColumn
                      << ", reading_100_dt=" << reading_100_dt.count()
                      // << ", reading_100_dt=" << reading_100_dt.count() / 100
                      // << ", reading_100_dt1=" << reading_100_dt1.count() / 100
                      << ", reading_100_dt1=" << reading_100_dt1.count()
                      // << ", reading_100_dt2=" << reading_100_dt2.count() / 100
                      << ", reading_100_dt2=" << reading_100_dt2.count()
                      << std::endl;

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

void DoSOmething()
{
  sleep(1);
  for(int i=0; i< 100; i++)
  {
    std::cerr << " writing file" << std::endl;
  }
}

int main(int argc, char **argv)
{
  // DoSOmething();

  /*
  TheMetadata m1;
  TheMetadata *m2 = &m1;
  MyMetadata my1;

  TheMetadata *m3 = &my1;

  auto a1 = m1.GetValue();
  auto a2 = m2->GetValue();
  auto a3 = m3->GetValue();
  */
  
  
  if (!std::filesystem::exists(FILE_NAME))
  {
    std::cerr << " writing file (wait what?" << std::endl;
    std::chrono::microseconds writing_dt;
    WriteTableToParquet(1000, 100, FILE_NAME, &writing_dt, 1);
  }
  ReadMetadata(FILE_NAME);

  /*
  std::cerr << " writing file" << std::endl;
  // if (!std::filesystem::exists(FILE_NAME))
  std::chrono::microseconds writing_dt;
  WriteTableToParquet(10000, 100, FILE_NAME, &writing_dt, 1);
  std::cerr << " reading file" << std::endl;
  // ReadFileContent(FILE_NAME);

  ReadMetadata(FILE_NAME);

  Status st = RunMain(argc, argv);
  if (!st.ok())
  {
    std::cerr << st << std::endl;
    return 1;
  }
  */
  return 0;
}