# ctypes_test.py
import ctypes
import pathlib

if __name__ == "__main__":
    # Load the shared library into ctypes
    libname = pathlib.Path().absolute() / "build/librapid_parquet.so"
    c_lib = ctypes.CDLL(libname)

result = c_lib.rapid_parquet.do_stuff()
print ("hello world:" + result)