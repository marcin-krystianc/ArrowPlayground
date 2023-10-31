cmake_minimum_required(VERSION 3.16)

project(arrow_benchmarks)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
message(STATUS "Parquet version: ${PARQUET_VERSION}")
message(STATUS "Parquet SO version: ${PARQUET_FULL_SO_VERSION}")

add_executable(arrow_benchmarks arrow_benchmarks.cc)
target_link_libraries(arrow_benchmarks PRIVATE Arrow::arrow_shared ArrowDataset::arrow_dataset_shared Parquet::parquet_shared)