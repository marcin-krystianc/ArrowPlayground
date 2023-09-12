#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <iostream>
#include <list>
#include <chrono>

using arrow::Status;

namespace
{
  const char *FILE_NAME = "my.parquet";

  Status WriteTableToParquet(const std::shared_ptr<arrow::Table> &table, const std::string &filename, std::chrono::microseconds *dt)
  {
    auto begin = std::chrono::steady_clock::now();
    auto result = arrow::io::FileOutputStream::Open(filename);
    auto outfile = result.ValueOrDie();
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 100000));
    auto end = std::chrono::steady_clock::now();
    *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return Status::OK();
  }

  std::shared_ptr<arrow::Table> GetTable(size_t n)
  {
    auto nRows = 10000;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    arrow::Int32Builder builder = arrow::Int32Builder();

    // For simplicity, we'll create int32 columns. You can expand this to handle other types.
    for (int i = 0; i < n; i++)
    {
      arrow::Int32Builder builder;
      for (auto j = 0; j < nRows; j++)
      {
        if (!builder.Append(j).ok())
          throw std::runtime_error("builder.Append");
      }

      std::shared_ptr<arrow::Array> array;
      if (!builder.Finish(&array).ok())
        throw std::runtime_error("builder.Finish");

      arrays.push_back(array);
      fields.push_back(arrow::field("int_column_" + std::to_string(i), arrow::int32()));
    }

    auto table = arrow::Table::Make(arrow::schema(fields), arrays);
    return table;
  }

  Status ReadEntireTable(const std::string &filename, std::chrono::microseconds *dt)
  {
    auto begin = std::chrono::steady_clock::now();

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    PARQUET_THROW_NOT_OK(reader->ReadTable(&parquet_table));

    auto end = std::chrono::steady_clock::now();
    *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return Status::OK();
  }

  Status ReadColumnsAsTable(const std::string &filename, std::vector<int> indicies, std::chrono::microseconds *dt)
  {
    auto begin = std::chrono::steady_clock::now();

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    PARQUET_THROW_NOT_OK(reader->ReadTable(indicies, &parquet_table));

    auto end = std::chrono::steady_clock::now();
    *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return Status::OK();
  }

  Status RunMain(int argc, char **argv)
  {
    std::list<int> nColumns = {100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000};
    std::vector<int> indicies(100);
    std::iota(indicies.begin(), indicies.end(), 0);

    for (int n : nColumns)
    {
      auto table = GetTable(n);
      std::chrono::microseconds writing_dt;
      ARROW_RETURN_NOT_OK(WriteTableToParquet(table, FILE_NAME, &writing_dt));

      std::chrono::microseconds reading_all_dt;
      ARROW_RETURN_NOT_OK(ReadEntireTable(FILE_NAME, &reading_all_dt));

      std::chrono::microseconds reading_100_dt;
      ARROW_RETURN_NOT_OK(ReadColumnsAsTable(FILE_NAME, indicies, &reading_100_dt));

      std::cerr << "n=" << n
                << " ,writing_dt=" << writing_dt.count() / n
                << " ,reading_all_dt=" << reading_all_dt.count() / n
                << " ,reading_100_dt=" << reading_100_dt.count() / 100
                << std::endl;
    }

    return Status::OK();
  }
} // namespace

int main(int argc, char **argv)
{
  Status st = RunMain(argc, argv);
  if (!st.ok())
  {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}