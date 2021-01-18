#!/usr/bin/env python

import setuptools

setuptools.setup(
    name="lite_migrate",
    description="Miniature migration library for SQLite databases",
    author="David Lougheed",
    author_email="david.lougheed@gmail.com",

    version="0.1.0-develop",
    python_requires=">=3.6",
    packages=["lite_migrate"],
    entry_points={
        "console_scripts": [

        ]
    },

    license="LGPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
    ]
)
