# from cython.cimports import crapid_parquet

# Declare the function signature with cdef for Cython optimization
cdef int calculate_factorial(int n):
    if n == 0:
        return 1
    else:
        return n * calculate_factorial(n - 1)

# Define a Python accessible function
def factorial(int number):
    if number < 0:
        raise ValueError("Factorial is not defined for negative numbers")
    return calculate_factorial(number)
