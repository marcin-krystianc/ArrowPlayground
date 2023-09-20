FROM ubuntu:23.04
RUN apt-get update && apt-get install -y \
# dotnet testing
	wget \
	nano \
	mc \
	htop \
	git \

# dotnet/runtime	
	clang \
	cmake \
	zlib1g-dev \
	libkrb5-dev \
	libssl-dev \

#others
	iputils-ping \
	strace \
	binutils \
	linux-tools-generic \
	valgrind \
	ninja-build \
	build-essential \
	net-tools \
	gdb \	
	python3-dev \
	python3-dbg \

#Clean apt cache
	&& rm -rf /var/lib/apt/lists/* \
	&& echo
	
# Set environment variables
ENV PYARROW_WITH_PARQUET=1 \
    PYARROW_WITH_DATASET=1 \
    PYARROW_WITH_PARQUET_ENCRYPTION=1


# Command to keep the container running
CMD ["bash"]