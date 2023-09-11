cmake_minimum_required(VERSION 3.16)

project(ColumnReadingPerf)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)

find_package(Arrow REQUIRED)

add_executable(ColumnReadingPerf ColumnReadingPerf.cc)
target_link_libraries(ColumnReadingPerf PRIVATE Arrow::arrow_shared)