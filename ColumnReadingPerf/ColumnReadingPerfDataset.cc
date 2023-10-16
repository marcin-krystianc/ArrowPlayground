// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/filesystem/localfs.h>
#include "arrow/compute/expression.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <cstdlib>
#include <iostream>

#include <iostream>
#include <list>
#include <chrono>
#include <random>
#include <vector>
#include <fstream>
#include <iomanip>

using arrow::Status;

// ***************************************************************************************************
//
// https://github.com/apache/arrow/blob/main/cpp/examples/arrow/dataset_parquet_scan_example.cc
//
// ***************************************************************************************************

/**
 * \brief Run Example
 *
 * Make sure there is a parquet dataset with the column_names
 * mentioned in the Configuration struct.
 *
 * Example run:
 * ./dataset_parquet_scan_example file:///<some-path>/data.parquet
 *
 */

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

namespace cp = arrow::compute;

const std::string FILE_NAME = "/tmp/my_cpp.parquet";

struct Configuration
{
  // Increase the ds::DataSet by repeating `repeat` times the ds::Dataset.
  size_t repeat = 1;

  // Indicates if the Scanner::ToTable should consume in parallel.
  bool use_threads = false;

  ds::InspectOptions inspect_options{};
  ds::FinishOptions finish_options{};
} conf;

arrow::Result<std::shared_ptr<fs::FileSystem>> GetFileSystemFromUri(
    const std::string &uri, std::string *path)
{
  return fs::FileSystemFromUri(uri, path);
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromDirectory(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string dir)
{
  // Find all files under `path`
  fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;

  ds::FileSystemFactoryOptions options;
  // The factory will try to build a child dataset.
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(fs, s, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(conf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RAISE(auto child, factory->Finish(conf.finish_options));

  ds::DatasetVector children{conf.repeat, child};
  auto dataset = ds::UnionDataset::Make(std::move(schema), std::move(children));

  return dataset;
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetParquetDatasetFromMetadata(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string metadata_path)
{
  ds::ParquetFactoryOptions options;
  ARROW_ASSIGN_OR_RAISE(
      auto factory, ds::ParquetDatasetFactory::Make(metadata_path, fs, format, options));
  return factory->Finish();
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromFile(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string file)
{
  ds::FileSystemFactoryOptions options;
  // The factory will try to build a child dataset.
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(fs, {file}, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(conf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RAISE(auto child, factory->Finish(conf.finish_options));

  ds::DatasetVector children;
  children.resize(conf.repeat, child);
  return ds::UnionDataset::Make(std::move(schema), std::move(children));
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromPath(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string path)
{
  ARROW_ASSIGN_OR_RAISE(auto info, fs->GetFileInfo(path));
  if (info.IsDirectory())
  {
    return GetDatasetFromDirectory(fs, format, path);
  }

  auto dirname_basename = arrow::fs::internal::GetAbstractPathParent(path);
  auto basename = dirname_basename.second;

  if (basename == "_metadata")
  {
    return GetParquetDatasetFromMetadata(fs, format, path);
  }

  return GetDatasetFromFile(fs, format, path);
}

arrow::Result<std::shared_ptr<ds::Scanner>> GetScannerFromDataset(std::shared_ptr<ds::Dataset> dataset, std::vector<std::string> columns, bool use_threads)
{
  ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());

  if (!columns.empty())
  {
    ARROW_RETURN_NOT_OK(scanner_builder->Project(columns));
  }

  scanner_builder->dataset_schema = nullptr;

  ARROW_RETURN_NOT_OK(scanner_builder->UseThreads(use_threads));

  return scanner_builder->Finish();
}

arrow::Result<std::shared_ptr<Table>> GetTableFromScanner(
    std::shared_ptr<ds::Scanner> scanner)
{
  return scanner->ToTable();
}

arrow::Status RunDatasetParquetScan(std::string path, std::vector<std::string> columns, std::chrono::microseconds *dt)
{
  auto begin = std::chrono::steady_clock::now();
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>(arrow::fs::LocalFileSystemOptions::Defaults());
  auto format = std::make_shared<ds::ParquetFileFormat>();
  ARROW_ASSIGN_OR_RAISE(auto dataset, GetDatasetFromPath(fs, format, path));
  ARROW_ASSIGN_OR_RAISE(auto scanner, GetScannerFromDataset(dataset, columns, conf.use_threads));
  ARROW_ASSIGN_OR_RAISE(auto table, GetTableFromScanner(scanner));
  auto end = std::chrono::steady_clock::now();
  // std::cout << "Table size, rows:" << table->num_rows() << ", columns:" << table->num_columns() << "\n";
  *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
  return arrow::Status::OK();
}

std::shared_ptr<arrow::Table> GetTable(size_t nColumns, size_t nRows)
{
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_real_distribution<> rand_gen(0.0, 1.0);

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> fields;

  // For simplicity, we'll create int32 columns. You can expand this to handle other types.
  for (int i = 0; i < nColumns; i++)
  {
    arrow::DoubleBuilder builder;
    for (auto j = 0; j < nRows; j++)
    {
      if (!builder.Append(rand_gen(rng)).ok())
        throw std::runtime_error("builder.Append");
    }

    std::shared_ptr<arrow::Array> array;
    if (!builder.Finish(&array).ok())
      throw std::runtime_error("builder.Finish");

    arrays.push_back(array);
    fields.push_back(arrow::field("c_" + std::to_string(i), arrow::float64(), false));
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
                        ->build();
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, chunkSize, properties));
  auto end = std::chrono::steady_clock::now();
  *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
  return Status::OK();
}

Status DoWork()
{
  std::ofstream csvFile;
  csvFile.open("results_cpp2.csv", std::ios_base::out); // append instead of overwrite
  csvFile << "columns, rows, chunk_size, writing(μs), reading_all(μs), reading_100(μs)" << std::endl;

  std::list<int> nColumns = {
      100, 200, 300, 400, 500, 600, 700, 800, 900,
      1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
      10000, 20000, 30000, 40000,
      50000
      };

  std::list<int64_t> chunk_sizes = {1000, 10000};
  std::list<int> rows_list = {5000};
  std::vector<int> indicies(100);
  std::iota(indicies.begin(), indicies.end(), 0);
  std::vector<std::string> columns;
  std::vector<std::string> empty_columns;

  for (auto i : indicies)
  {
    columns.push_back("c_" + std::to_string(i));
  }

  for (auto chunk_size : chunk_sizes)
  {
    for (int nRow : rows_list)
    {
      for (int nColumn : nColumns)
      {
        std::cerr << "Writing table" << std::endl;

        std::chrono::microseconds writing_dt;
        ARROW_RETURN_NOT_OK(WriteTableToParquet(nColumn, nRow, FILE_NAME, &writing_dt, chunk_size));

        std::cerr << "Reading columns" << std::endl;
        const int repeats = 3;
        std::vector<std::chrono::microseconds> reading_all_dts(repeats);
        std::vector<std::chrono::microseconds> reading_100_dts(repeats);
        for (int i = 0; i < repeats; i++)
        {
          ARROW_RETURN_NOT_OK(RunDatasetParquetScan(FILE_NAME, empty_columns, &reading_all_dts[i]));
          ARROW_RETURN_NOT_OK(RunDatasetParquetScan(FILE_NAME, columns, &reading_100_dts[i]));
        }

        auto reading_all_dt = *std::min_element(reading_all_dts.begin(), reading_all_dts.end());
        auto reading_100_dt = *std::min_element(reading_100_dts.begin(), reading_100_dts.end());

        std::cerr << "(" << nColumn << ", " << nRow << ")"
                  << ", chunk_size=" << chunk_size
                  //<< ", writing_dt=" << writing_dt.count() / nColumn
                  << ", reading_all_dt=" << reading_all_dt.count() / nColumn
                  << ", reading_100_dt=" << reading_100_dt.count() / 100
                  << std::endl;

        csvFile << nColumn << ","
                << nRow << ","
                << chunk_size << ","
                << writing_dt.count() << ","
                << reading_all_dt.count() << ","
                << reading_100_dt.count()
                << std::endl;
      }
    }
  }

  return Status::OK();
} // namespa

int main(int argc, char **argv)
{
  auto status = DoWork();
  if (!status.ok())
  {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}