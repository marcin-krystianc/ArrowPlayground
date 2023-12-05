# ctypes_test.py
import ctypes
import pathlib
import rapidparquet as rp

if __name__ == "__main__":
    # Load the shared library into ctypes
    libname = pathlib.Path().absolute() / "build/librapid_parquet.so"
    c_lib = ctypes.CDLL(libname)

# result = c_lib.rapid_parquet.do_stuff()
print ("hello world:")
result = rp.my_add(2,3)
metada = rp.my_metadata('/workspace/tmp/my.parquet')
result = rp.factorial(5)
{}