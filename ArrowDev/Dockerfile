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
	make \

	# perf
	# xz-utils \
    # flex \
    # bison \
    # libelf-dev \
    # systemtap-sdt-dev \
    # libslang2-dev \
    # libcap-dev \
    # libperl-dev \
    # libpfm4-dev \
    # libnuma-dev \
    # libtraceevent-dev \
    
	#Clean apt cache
	&& rm -rf /var/lib/apt/lists/* \	
	&& echo

# WORKDIR /
# RUN wget https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.4.16.tar.xz && \
#     tar -xf ./linux-6.4.16.tar.xz && \
#     cd linux-6.4.16/tools/perf/ && \
#     make -C . && make install

# Clone the specified Arrow repository and checkout the desired branch
WORKDIR /src
RUN git clone --recursive https://github.com/apache/arrow.git && \
    cd arrow && \
    git remote add marcin-krystianc https://github.com/marcin-krystianc/arrow.git && \
	git fetch --all && \
	git checkout --recurse-submodules apache-arrow-17.0.0

# Build Arrow C++
WORKDIR /src/arrow/cpp
RUN mkdir build && cd build && \
    cmake .. \
	-DARROW_PARQUET=ON \
    -DPARQUET_BUILD_EXECUTABLES=ON \
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
	-DCMAKE_BUILD_TYPE="RelWithDebInfo" \
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
    -DARROW_S3=ON \
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

# Remove any previous version of Arrow Python package
RUN find /usr/local/lib/ -depth -name pyarrow -type d -print -exec rm -r {} \;

# Build Arrow Python
WORKDIR /src/arrow/python
RUN pip3 install cython && \
    pip3 install jupyterlab polars parquet-tools build && \
    python3 setup.py build_ext --inplace --with-parquet-encryption --with-dataset --with-parquet && \
    python3 setup.py install

# Cleanup Arrow Python build artifacts
RUN rm -rf /src/arrow/python/build

# Cleanup apt cache to further reduce image size

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

WORKDIR /src
RUN git clone https://github.com/adamreeve/parquet-inspect.git && \
    cd parquet-inspect && \
	python3 -m build  && \
	pip3 install --editable .

WORKDIR /src/arrow/cpp/build
ENV ARROW_TEST_DATA=/src/arrow/testing/data
ENV PARQUET_TEST_DATA=/src/arrow/cpp/submodules/parquet-testing/data

# Command to keep the container running
CMD ["bash"]