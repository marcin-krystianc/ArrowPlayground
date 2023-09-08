# Use the specified base image
FROM apache/arrow-dev:amd64-ubuntu-22.04-python-3
# Set environment variables
ENV PYARROW_WITH_PARQUET=1 \
    PYARROW_WITH_DATASET=1 \
    PYARROW_WITH_PARQUET_ENCRYPTION=1

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    ninja-build \
    cmake \
    git \
	wget \
	nano \
	mc \
	htop \
	ninja-build \
	net-tools \
	gdb \
	iputils-ping \
	strace \
	binutils \
	linux-tools-generic \
	valgrind \

	#Clean apt cache
	&& rm -rf /var/lib/apt/lists/* \	
	&& echo

# Clone the specified Arrow repository and checkout the desired branch
WORKDIR /src
RUN git clone https://github.com/apache/arrow.git && \
    cd arrow 
# Build Arrow C++
WORKDIR /src/arrow/cpp
RUN mkdir build && cd build && \
    cmake .. \
    -DARROW_PARQUET=ON \
    -DPARQUET_REQUIRE_ENCRYPTION=ON \
    -DARROW_DATASET=ON \
    -DARROW_ACERO=ON \
    -DARROW_ALTIVEC=ON \
    -DARROW_AZURE=OFF \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BROTLI_USE_SHARED=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_CONFIG_SUMMARY_JSON=ON \
    -DARROW_BUILD_INTEGRATION=ON \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_TESTS=ON \
    -DARROW_BZ2_USE_SHARED=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DEFINE_OPTIONS=ON \
    -DARROW_DEPENDENCY_SOURCE=AUTO \
    -DARROW_DEPENDENCY_USE_SHARED=ON \
    -DARROW_ENABLE_TIMING_TESTS=ON \
    -DARROW_EXTRA_ERROR_CONTEXT=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_GFLAGS_USE_SHARED=ON \
    -DARROW_GGDB_DEBUG=ON \
    -DARROW_GRPC_USE_SHARED=ON \
    -DARROW_IPC=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_JEMALLOC_USE_SHARED=ON \
    -DARROW_JSON=ON \
    -DARROW_LZ4_USE_SHARED=ON \
    -DARROW_MIMALLOC=ON \
    -DARROW_OPENSSL_USE_SHARED=ON \
    -DARROW_ORC=ON \
    -DARROW_POSITION_INDEPENDENT_CODE=ON \
    -DARROW_PROTOBUF_USE_SHARED=ON \
    -DARROW_RE2_LINKAGE=static \
    -DARROW_RUNTIME_SIMD_LEVEL=MAX \
    -DARROW_S3=OFF \
    -DARROW_SIMD_LEVEL=DEFAULT \
    -DARROW_SNAPPY_USE_SHARED=ON \
    -DARROW_SUBSTRAIT=ON \
    -DARROW_TEST_LINKAGE=shared \
    -DARROW_THRIFT_USE_SHARED=ON \
    -DARROW_USE_CCACHE=ON \
    -DARROW_UTF8PROC_USE_SHARED=ON \
    -DARROW_WITH_BACKTRACE=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_RE2=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_UTF8PROC=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_ZSTD_USE_SHARED=ON \
    -GNinja && \
    ninja install
# Cleanup Arrow C++ build directory
RUN rm -rf /src/arrow/cpp/build
# Build Arrow Python
WORKDIR /src/arrow/python
RUN python setup.py build_ext --inplace --with-parquet-encryption --with-dataset --with-parquet && \
    python setup.py install
# Cleanup Arrow Python build artifacts
RUN rm -rf /src/arrow/python/build
# Cleanup apt cache to further reduce image size
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
RUN pip3 install jupyterlab
# Command to keep the container running
CMD ["bash"]