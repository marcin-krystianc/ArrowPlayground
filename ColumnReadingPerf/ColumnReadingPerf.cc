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

  Status RunMain(int argc, char **argv)
  {

    std::list<int> nColumns = {100, 200, 300, 500, 1000, 10000};
    for (int n : nColumns)
    {
      auto builder = arrow::Int32Builder();

      for (int i = 0; i < n; i++)
      {
        builder.Append(1);
      }

      auto array = builder.Finish();

      std::cerr << "* n:" << n << std::endl;

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