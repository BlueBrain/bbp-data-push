#!/usr/bin/env python

import imp

from setuptools import setup, find_packages

VERSION = imp.load_source("", "bba_dataset_push/__init__.py").__version__

setup(
    name="bba_data_push",
    author="Blue Brain Project, EPFL",
    version=VERSION,
    description=(
        "Package creating resource payloads from atlas datasets and push them along "
        "with the corresponding dataset files into Nexus."
    ),
    download_url="ssh://bbpcode.epfl.ch/code/a/dke/bba_data_integrity_check",
    license="BBP-internal-confidential",
    python_requires=">=3.6.0",
    install_requires=[
        "nexusforge>=0.5.0",
        "click>=7.0",
        "numpy>=1.19",
        "h5py>=3.1.0",
        "pynrrd>=0.4.0",
        "PyYAML>=5.3.1",
        "PyJWT>=2.0.0",
    ],
    extras_require={
        "dev": ["pytest>=4.3", "pytest-cov==2.10.0"],
    },
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        "console_scripts": ["bba-data-push=bba_dataset_push.bba_data_push:start"]
    },
)
