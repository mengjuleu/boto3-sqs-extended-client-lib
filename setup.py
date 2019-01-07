#!/usr/bin/env python

from setuptools import setup, find_packages

description = 'A Extended SQS Client Library for Python.'
setup(name='extendedsqsclient',
      version='1.0',
      packages=find_packages(exclude=['tests']),
      description=description,
      author='Meng-Ju Leu',
      author_email='mleu@atlassian.com',
      requires=['boto3'],
      install_requires=['boto3'])
