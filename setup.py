# coding: utf-8

from setuptools import find_packages, setup

import io
import os

# 包元信息
NAME = 'mega-spark'
DESCRIPTION = 'Some computing tools about feature tool and ml model.'
AUTHOR = 'huangning'

# 项目运行需要的依赖
REQUIRES = ["pyspark>=2.4.0,<2.4.7"
            ]

here = os.path.abspath(os.path.dirname(__file__))

try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except IOError:
    long_description = DESCRIPTION

setup(
      name=''+NAME,
      version='1.0.0',
      description=DESCRIPTION,
      long_description=long_description,
      long_description_content_type="text/markdown",
      author=AUTHOR,
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
      ],
      packages=find_packages(),
      install_requires=REQUIRES,
      python_requires='>=2.7,<=3.7.*',
      include_package_data=True
)
