cmake_minimum_required(VERSION 3.23)
project(cudf_example LANGUAGES CXX CUDA)

find_package(cudf REQUIRED)

add_executable(cudf_example main.cpp)

target_link_libraries(cudf_example
  PRIVATE cudf::cudf
)

set_target_properties(cudf_example PROPERTIES
  CXX_STANDARD 17
  CUDA_STANDARD 17
)