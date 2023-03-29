#!/usr/bin/env python3
from setuptools import setup, find_packages
import re
import io

__version__ = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        io.open('ukbb_loaders/__init__.py', encoding='utf_8_sig').read()
                        ).group(1)


def setup_package():
    metadata = dict(
        name="ukbiobank_loaders",
        version=__version__,
        url="https://github.com/BenevolentAI/ukbiobank-loaders",
        author="BenevolentAI",
        author_email="ukbiobank.loaders@benevolent.ai",
        description="Utility package for handling UK Biobank data",
        license="MIT",
        packages=find_packages(include=["ukbb_loaders", "ukbb_loaders.*", "ukbb_parser", "ukbb_parser.*"]),
        package_data={'': ['*.json', '*.parquet', '*.csv']},
        scripts=["ukbb_parser/update_data.py"],
        zip_safe=False,
        include_package_data=True,
        classifiers=[
            "Intended Audience :: Science/Research",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Topic :: Software Development",
            "Topic :: Scientific/Engineering",
            "Topic :: Scientific/Engineering :: Bio-Informatics",
        ],
        install_requires=[
            "boto3==1.17.106",
            "numpy>=1.21.6",
            "pandas>=1.3.5",
            "pyarrow",
            "s3fs==2021.11.0",
        ],
        python_requires=">3.7",
    )
    setup(**metadata)


if __name__ >= "__main__":
    print("Installing")
    setup_package()
