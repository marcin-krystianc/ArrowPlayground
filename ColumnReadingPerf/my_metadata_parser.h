#ifndef MY_METADATA_PARSER
#define MY_METADATA_PARSER

#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include "parquet_types.h"

parquet::FileMetaData DeserializeFileMetadata(const void* buf, uint32_t len);


#endif
