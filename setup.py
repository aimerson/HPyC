#! /usr/bin/env python

from setuptools import setup, find_packages

setup(name='galacticus',
      version='0.1',
      description='Python package for submitting/managing HPC jobs.',
      url='https://github.com/aimerson/HPyC',
      author='Alex Merson',
      author_email='alex.i.merson@gmail.com',
      license='MIT',
      packages=find_packages(),
      package_dir={'hpyc':'hpyc'},
      zip_safe=False)

