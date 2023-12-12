#!/usr/bin/env python
import os
import sys
from codecs import open

from setuptools import setup
from distutils.extension import Extension
from setuptools.command.test import test as TestCommand
from Cython.Build import cythonize
import pyarrow
import numpy

CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 7)

if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write(
        """
==========================
Unsupported Python version
==========================
This version of RapidParquet requires at least Python {}.{}, but
you're trying to install it on Python {}.{}. To resolve this,
consider upgrading to a supported Python version.

If you can't upgrade your Python version, you'll need to
pin to an older version of Requests (<2.28).
""".format(
            *(REQUIRED_PYTHON + CURRENT_PYTHON)
        )
    )
    sys.exit(1)


class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass into py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        try:
            from multiprocessing import cpu_count

            self.pytest_args = ["-n", str(cpu_count()), "--boxed"]
        except (ImportError, NotImplementedError):
            self.pytest_args = ["-n", "1", "--boxed"]

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)



about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "rapidparquet", "__version__.py"), "r", "utf-8") as f:
    exec(f.read(), about)

with open("README.md", "r", "utf-8") as f:
    readme = f.read()

# Define your extension
extensions = [
    Extension(
        "rapidparquet.my_cython_code",
        ["rapidparquet/my_cython_code.pyx"],
        include_dirs = [pyarrow.get_include(), numpy.get_include()],  
        library_dirs = pyarrow.get_library_dirs(),
    )
]

# Make default named pyarrow shared libs available.
pyarrow.create_library_symlinks()

setup(
    name=about["__title__"],
    version=about["__version__"],
    description=about["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    author=about["__author__"],
    author_email=about["__author_email__"],
    url=about["__url__"],
    packages=["rapidparquet"],
    package_data={"": ["LICENSE", "NOTICE"]},
    package_dir={"": "."},
    include_package_data=True,
    python_requires=">=3.7",
    setup_requires="pyarrow>=10,",
    install_requires="pyarrow>=10",
    extras_require={"arrow": ["pyarrow>=7.0,<15"], "numpy": "numpy>=1.20.0"},
    license=about["__license__"],
    zip_safe=False,
    ext_modules=cythonize(extensions),    
    project_urls={
        "Documentation": "https://github.com/marcin-krystianc/ArrowPlayground",
        "Source": "https://github.com/marcin-krystianc/ArrowPlayground"
    },
)