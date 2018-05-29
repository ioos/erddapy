from __future__ import (absolute_import, division, print_function)

import os
from codecs import open

from setuptools import find_packages, setup

import versioneer

rootpath = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with open(os.path.join(rootpath, *parts), 'rb') as f:
        return f.read().decode('utf-8')


with open('requirements.txt') as f:
    requires = f.readlines()
install_requires = [req.strip() for req in requires]


setup(
    name='erddapy',
    version=versioneer.get_version(),
    description='Python interface for ERDDAP',
    long_description=f'{read("README.md")}',
    long_description_content_type='text/markdown',
    author='Filipe Fernandes',
    author_email='ocefpaf@gmail.com',
    url='https://github.com/pyoceans/erddapy',
    keywords=['ERDDAP', 'Scientific Python', 'Remote data access'],
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: BSD License',
        'Development Status :: 4 - Beta'],
    packages=find_packages(),
    extras_require={
        'testing': ['pytest'],
    },
    license=f'{read("LICENSE.txt")}',
    install_requires=install_requires,
    cmdclass=versioneer.get_cmdclass(),
)
