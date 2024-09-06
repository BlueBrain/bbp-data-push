#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="bba_data_push",
    author="Blue Brain Project, EPFL",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description=(
        "Package creating resource payloads from atlas datasets and push them along "
        "with the corresponding dataset files into Nexus."
    ),
    license="Apache-2",
    python_requires=">=3.7",
    install_requires=[
        "nexusforge>=0.8.2",
        "click>=7.0",
        "numpy>=1.19",
        "h5py>=2.10.0",
        "pynrrd>=0.4.0",
        "PyYAML>=5.3.1",
        "PyJWT>=2.0.0",
        "rdflib>=6.2.0",
        "urllib3",
        "voxcell"
    ],
    extras_require={
        "dev": ["pytest>=7.0.1", "pytest-cov>=2.8.0"],
    },
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        "console_scripts": ["bba-data-push=bba_data_push.bba_dataset_push:start"]
    },
)
