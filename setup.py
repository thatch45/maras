#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Import python libs
import os
import sys
# Import maras libs
import maras

if 'USE_SETUPTOOLS' in os.environ or 'setuptools' in sys.modules:
    from setuptools import setup
else:
    from distutils.core import setup

NAME = 'maras'
DESC = ('Pure python database engine')
VERSION = maras.version

setup(name=NAME,
      version=VERSION,
      description=DESC,
      author='Thomas S Hatch',
      author_email='thatch@saltstack.com',
      url='https://github.com/thatch45/maras',
      classifiers=[
          'Operating System :: OS Independent',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          ],
      packages=[
          'maras',
          'maras.index',
          'maras.db',
          'maras.utils',
          ]
      )
