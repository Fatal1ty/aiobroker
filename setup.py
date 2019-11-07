#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name="aiobroker",
    version="0.4",
    description="Message broker to organize the communication of components with each other",
    platforms="all",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Development Status :: 4 - Beta",
    ],
    license="Apache License, Version 2.0",
    author="Alexander Tikhonov",
    author_email="random.gauss@gmail.com",
    url='https://github.com/Fatal1ty/aiobroker',
    packages=find_packages(exclude=("tests",)),
    python_requires=">=3.6",
)
