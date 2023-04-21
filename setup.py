#!/usr/bin/env python
from setuptools import setup, find_packages
from os.path import join, dirname

with open("README.md", "r") as fh:
    long_description = fh.read()

requirementstxt = join(dirname(__file__), "requirements.txt")
with open(requirementstxt, "r") as file:
    requirements = [line.strip() for line in file if line.strip()]

setup(
    name='FECdownload',
    version=0.1,
    description='Simple script for downloading individual contributions from the OpenFEC API.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Jarno Rantaharju',
    author_email='jarno.rantaharju@aalto.fi',
    url='https://github.com/rantahar/fec_download',
    packages=find_packages(where='.'),
    scripts=['download_scheduleA.py'],
    python_requires=">=3.6",
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
)