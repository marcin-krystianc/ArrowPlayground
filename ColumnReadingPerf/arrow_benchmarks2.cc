
#include "my_metadata_parser.h"

#include <iostream>
#include <list>
#include <chrono>
#include <random>
#include <vector>
#include <fstream>
#include <filesystem>
#include <iomanip>

#include <unistd.h>

#include "arrow/status.h"

std::vector<char> ReadFile(const std::string &filename)
{
  auto begin = std::chrono::steady_clock::now();
  std::ifstream fs(filename, std::ios::in | std::ios::binary);
  fs.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  fs.seekg(0, std::ios::end);
  size_t length = fs.tellg();
  fs.seekg(0, std::ios::beg);
  std::vector<char> buffer(length);
  fs.read(&buffer[0], length);
  return buffer;
}

arrow::Status ReadMetadata_CustomThrift(const std::string &filename)
{
  for (int i = 0; i < 500; i++)
  {
    auto buffer = ReadFile(filename);
    uint32_t len = buffer.size();

    auto begin = std::chrono::steady_clock::now();
    auto metadata = DeserializeFileMetadata((const void*)&buffer[0], len);
    auto end = std::chrono::steady_clock::now();

    auto rows = metadata.num_rows;
    auto row_groups = metadata.row_groups;

  }
  
  return arrow::Status::OK();
}