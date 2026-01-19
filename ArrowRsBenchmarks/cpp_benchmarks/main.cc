// main.cpp
/*
> g++ -std=c++17 -O2 main.cc -o cpp_benchmarks \
  -I/usr/local/lib/python3.10/dist-packages/pyarrow/include \
  -L/usr/local/lib/python3.10/dist-packages/pyarrow \
  -larrow -lparquet

> LD_LIBRARY_PATH=/usr/local/lib/python3.10/dist-packages/pyarrow ./cpp_benchmarks

*/ 
#include <arrow/io/file.h>
#include <parquet/file_reader.h>

#include <chrono>
#include <iostream>
#include <memory>

int main() {
    const std::string file_path = "/tmp/my.parquet";

    std::cout << "Attempting to read metadata from: " << file_path
              << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 1000; ++i) {
        // 1. Open the Parquet file as an Arrow readable file
        // Arrow uses Result<T> return type, not output parameters
        arrow::Result<std::shared_ptr<arrow::io::ReadableFile>>
            result = arrow::io::ReadableFile::Open(file_path);

        if (!result.ok()) {
            std::cerr << "Failed to open file: "
                      << result.status().ToString() << std::endl;
            return 1;
        }

        std::shared_ptr<arrow::io::ReadableFile> infile =
            result.ValueOrDie();

        // 2. Create a Parquet file reader
        std::unique_ptr<parquet::ParquetFileReader> reader =
            parquet::ParquetFileReader::Open(infile);

        // 3. Access file metadata
        std::shared_ptr<parquet::FileMetaData> file_metadata =
            reader->metadata();

        // (Nothing else needed, just touching metadata)
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            end - start);

    std::cout << "Milliseconds: " << duration.count() << " ms"
              << std::endl;

    std::cout << "\nSuccessfully read Parquet metadata!" << std::endl;

    return 0;
}