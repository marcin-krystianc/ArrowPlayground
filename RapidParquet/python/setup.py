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

# https://cython.readthedocs.io/en/latest/src/userguide/source_files_and_compilation.html#distributing-cython-modules
def no_cythonize(extensions, **_ignore):
    for extension in extensions:
        sources = []
        for sfile in extension.sources:
            path, ext = os.path.splitext(sfile)
            if ext in (".pyx", ".py"):
                if extension.language == "c++":
                    ext = ".cpp"
                else:
                    ext = ".c"
                sfile = path + ext
            sources.append(sfile)
        extension.sources[:] = sources
    return extensions

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
    Extension( "rapidparquet", ["rapidparquet/rapid_parquet_cython.pyx"],
        include_dirs = [pyarrow.get_include(), numpy.get_include()],  
        library_dirs = pyarrow.get_library_dirs(),
        language = "c++",
    )
]

CYTHONIZE = bool(int(os.getenv("CYTHONIZE", 0))) and cythonize is not None
CYTHONIZE = 1

if CYTHONIZE:
    compiler_directives = {"language_level": 3, "embedsignature": True}
    extensions = cythonize(extensions, compiler_directives=compiler_directives)
else:
    extensions = no_cythonize(extensions)

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
    package_dir={"": "."},
    include_package_data=True,
    python_requires=">=3.7",
    setup_requires=["pyarrow>=10","cython>=3"],
    zip_safe=False,
    ext_modules=extensions,    
    project_urls={
        "Documentation": "https://github.com/marcin-krystianc/ArrowPlayground",
        "Source": "https://github.com/marcin-krystianc/ArrowPlayground"
    },
)