import io
import os

from setuptools import find_packages, setup

import versioneer

rootpath = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with open(os.path.join(rootpath, *parts), "rb") as f:
        return f.read().decode("utf-8")


with io.open("requirements.txt") as f:
    requires = f.readlines()
install_requires = [req.strip() for req in requires]


setup(
    name="erddapy",
    version=versioneer.get_version(),
    description="Python interface for ERDDAP",
    long_description=f'{read("README.md")}',
    long_description_content_type="text/markdown",
    author="Filipe Fernandes",
    author_email="ocefpaf@gmail.com",
    url="https://github.com/ioos/erddapy",
    keywords=["ERDDAP", "Scientific Python", "Remote data access"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: BSD License",
    ],
    packages=find_packages(),
    extras_require={"testing": ["pytest"]},
    install_requires=install_requires,
    cmdclass=versioneer.get_cmdclass(),
)
