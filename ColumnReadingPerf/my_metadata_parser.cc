// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "parquet/exception.h"

#include "parquet_types.h"

#include <sstream>


constexpr int32_t kDefaultThriftStringSizeLimit = 100 * 1000 * 1000;
// Structs in the thrift definition are relatively large (at least 300 bytes).
// This limits total memory to the same order of magnitude as
// kDefaultStringSizeLimit.
constexpr int32_t kDefaultThriftContainerSizeLimit = 1000 * 1000;

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

std::shared_ptr<ThriftBuffer> CreateReadOnlyMemoryBuffer(uint8_t* buf, uint32_t len) {
#if PARQUET_THRIFT_VERSION_MAJOR > 0 || PARQUET_THRIFT_VERSION_MINOR >= 14
    auto conf = std::make_shared<apache::thrift::TConfiguration>();
    conf->setMaxMessageSize(std::numeric_limits<int>::max());
    return std::make_shared<ThriftBuffer>(buf, len, ThriftBuffer::OBSERVE, conf);
#else
    return std::make_shared<ThriftBuffer>(buf, len);
#endif
  }

template <class T>
void DeserializeUnencryptedMessage(const uint8_t* buf, uint32_t* len,
                                     T* deserialized_msg) {
    // Deserialize msg bytes into c++ thrift msg using memory transport.
    auto tmem_transport = CreateReadOnlyMemoryBuffer(const_cast<uint8_t*>(buf), *len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    // Protect against CPU and memory bombs
    tproto_factory.setStringSizeLimit(kDefaultThriftStringSizeLimit);
    tproto_factory.setContainerSizeLimit(kDefaultThriftContainerSizeLimit);
    auto tproto = tproto_factory.getProtocol(tmem_transport);
    try {
      deserialized_msg->read(tproto.get());
    } catch (std::exception& e) {
      std::stringstream ss;
      ss << "Couldn't deserialize thrift: " << e.what() << "\n";
      throw parquet::ParquetException(ss.str());
    }
    uint32_t bytes_left = tmem_transport->available_read();
    *len = *len - bytes_left;
  }
  
my_parquet::FileMetaData DeserializeFileMetadata(const void* buf, uint32_t len)
{
    my_parquet::FileMetaData fileMetaData;
    DeserializeUnencryptedMessage ((const uint8_t*)buf, &len, &fileMetaData);
    return fileMetaData;
}
