// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <iostream>
#include <list>

using arrow::Status;

namespace
{

  Status WriteTableToParquet(const std::shared_ptr<arrow::Table> &table, const std::string &filename)
  {
    /*
    // Choose compression
    std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(filename, &outfile));

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table,
                                                   arrow::default_memory_pool(), outfile,
                                                   3, props, arrow_props));
    return Status::OK();
*/

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(filename, &outfile));
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table.get(), arrow::default_memory_pool(), outfile, 1024));
        return Status::OK();

  }

  Status RunMain(int argc, char **argv)
  {
    auto nRows = 1;
    std::list<int> nColumns = {100, 200, 300, 500, 1000, 10000};

    for (int n : nColumns)
    {
      std::cerr << "* n1:" << n << std::endl;
      std::vector<std::shared_ptr<arrow::Array>> arrays;
      std::vector<std::shared_ptr<arrow::Field>> fields;
      arrow::Int32Builder builder = arrow::Int32Builder();

      // For simplicity, we'll create int32 columns. You can expand this to handle other types.
      for (int i = 0; i < n; i++)
      {
        arrow::Int32Builder builder;
        for (auto j = 0; j < nRows; j++)
        {
          ARROW_RETURN_NOT_OK(builder.Append(j));
        }

        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder.Finish(&array));

        arrays.push_back(array);
        fields.push_back(arrow::field("int_column_" + std::to_string(i), arrow::int32()));
      }

      auto table = arrow::Table::Make(arrow::schema(fields), arrays);

      ARROW_RETURN_NOT_OK(WriteTableToParquet(table, "my.parquet"));

      std::cerr << "* n4:" << n << std::endl;
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