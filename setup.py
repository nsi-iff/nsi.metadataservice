#encoding: utf-8

from setuptools import setup, find_packages
import sys, os

version = '0.0.1'

setup(name='nsi.metadataservice',
      version=version,
      description="A package to extract documents metadata.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='João Felipe Roque Moraes',
      author_email='jfelipe.roque@gmail.com',
      url='',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
          # -*- Extra requirements: -*-
          'cyclone',
      ],
      entry_points="""
# -*- Entry points: -*-
""",
      )
