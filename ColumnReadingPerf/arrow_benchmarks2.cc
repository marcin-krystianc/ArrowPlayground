
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

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "arrow/status.h"

constexpr int32_t kDefaultThriftStringSizeLimit = 100 * 1000 * 1000;
// Structs in the thrift definition are relatively large (at least 300 bytes).
// This limits total memory to the same order of magnitude as
// kDefaultStringSizeLimit.
constexpr int32_t kDefaultThriftContainerSizeLimit = 1000 * 1000;

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

std::shared_ptr<ThriftBuffer> CreateReadOnlyMemoryBuffer(uint8_t* buf, uint32_t len);

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

size_t WriteListBegin(void* dst, const ::apache::thrift::protocol::TType elemType, const uint32_t size)
{
  std::shared_ptr<ThriftBuffer> mem_buffer(new ThriftBuffer(16));
  apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
  // Protect against CPU and memory bombs
  tproto_factory.setStringSizeLimit(kDefaultThriftStringSizeLimit);
  tproto_factory.setContainerSizeLimit(kDefaultThriftContainerSizeLimit);
  auto tproto = tproto_factory.getProtocol(mem_buffer);
  mem_buffer->resetBuffer();
  tproto->writeListBegin(elemType, static_cast<uint32_t>(size));
  uint8_t* ptr;
  uint32_t len;
  mem_buffer->getBuffer(&ptr, &len);
  memcpy(dst, ptr, len);
  return len;
}

arrow::Status ReadMetadata_CustomThrift(const std::string &filename)
{
  auto buffer_src = ReadFile(filename);
  uint32_t len = buffer_src.size();

  auto begin = std::chrono::steady_clock::now();
  auto metadata = DeserializeFileMetadata((const void*)&buffer_src[0], len);
  auto end = std::chrono::steady_clock::now();
  auto parserData = gParserData;
  auto rows = metadata.num_rows;
  auto row_groups = metadata.row_groups.size();
  auto columns = metadata.schema.size() - 1;

  std::vector<size_t> columns_to_copy = {1, 5};
  std::vector<char> buffer_dst(buffer_src.size());
  
  uint32_t index_src = 0;
  uint32_t index_dst = 0;
  size_t toCopy = 0;

  toCopy = gParserData.schema_list_offsets[0] - index_src;
  memcpy(&buffer_dst[index_dst], &buffer_src[index_src], toCopy);
  index_src += toCopy;
  index_dst += toCopy;

  index_dst += WriteListBegin(&buffer_dst[index_dst], ::apache::thrift::protocol::T_STRUCT, columns_to_copy.size() + 1); // one extra element for root
  index_src = gParserData.schema_list_offsets[1];

  toCopy = gParserData.schema_elements[0] - index_src;
  memcpy(&buffer_dst[index_dst], &buffer_src[index_src], toCopy); // root elemnt (between list end and first stored column element)
  index_dst += toCopy;

  for (auto column : columns_to_copy)
  {
    toCopy = gParserData.schema_elements[column + 1] - gParserData.schema_elements[column];
    memcpy(&buffer_dst[index_dst], &buffer_src[gParserData.schema_elements[column]], toCopy);
    index_dst += toCopy;
  }

  index_src = gParserData.schema_elements[columns];

  for (auto row_group = 0; row_group < row_groups; row_group++)
  {      
      auto row_group_offset = gParserData.row_groups[row_group];
      toCopy = row_group_offset + gParserData.column_chunks_list_offsets[row_group * 2 + 0] - index_src;
      memcpy(&buffer_dst[index_dst], &buffer_src[index_src], toCopy);
      index_dst += toCopy;

      index_dst += WriteListBegin(&buffer_dst[index_dst], ::apache::thrift::protocol::T_STRUCT, columns_to_copy.size());

      for (auto column_to_copy : columns_to_copy)
      {
        toCopy = gParserData.column_chunks[(columns + 1) * row_group + column_to_copy + 1] - gParserData.column_chunks[(columns + 1) * row_group + column_to_copy];
        memcpy(&buffer_dst[index_dst], &buffer_src[row_group_offset + gParserData.column_chunks[(columns + 1) * row_group + column_to_copy]], toCopy);
        index_dst += toCopy;
      }

      index_src = row_group_offset + gParserData.column_chunks[(columns + 1) * row_group + columns + 1];
  }

  // Copy left overs
  toCopy = buffer_src.size() - index_src;
  memcpy(&buffer_dst[index_dst], &buffer_src[index_src], toCopy);
  index_dst += toCopy;

  // try it
  metadata = DeserializeFileMetadata((const void*)&buffer_dst[0], index_dst);

  return arrow::Status::OK();
}