# https://stackoverflow.com/questions/17511496/how-to-create-a-shared-library-with-cmake
cmake_minimum_required(VERSION 3.16)

project(rapid_parquet VERSION 1.0.1 DESCRIPTION "mylib description")

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
message(STATUS "Parquet version: ${PARQUET_VERSION}")
message(STATUS "Parquet SO version: ${PARQUET_FULL_SO_VERSION}")

add_library(rapid_parquet SHARED rapid_parquet.cc)
target_link_libraries(rapid_parquet PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
set_target_properties(rapid_parquet PROPERTIES PUBLIC_HEADER rapid_parquet.h)
include(GNUInstallDirs)
install(TARGETS rapid_parquet
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

configure_file(rapid_parquet.pc.in rapid_parquet.pc @ONLY)
install(FILES ${CMAKE_BINARY_DIR}/rapid_parquet.pc DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/pkgconfig)
