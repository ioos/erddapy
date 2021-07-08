<h1 align="center" style="margin:1em;">
  <a href="https://ioos.github.io/erddapy/">erddapy</a>
</h1>

<h4 align="center">
erddapy: ERDDAP + Python.
</h4>

<p align="center">
<a href="https://anaconda.org/conda-forge/erddapy">
<img src="https://img.shields.io/conda/dn/conda-forge/erddapy.svg"
 alt="conda-forge downloads" /></a>
<a href="https://github.com/ioos/erddapy/releases">
<img src="https://img.shields.io/github/tag/ioos/erddapy.svg"
 alt="Latest version" /></a>
<a href="https://github.com/ioos/erddapy/commits/main">
<img src="https://img.shields.io/github/commits-since/ioos/erddapy/latest.svg"
 alt="Commits since last release" /></a>
<a href="https://github.com/ioos/erddapy/graphs/contributors">
<img src="https://img.shields.io/github/contributors/ioos/erddapy.svg"
 alt="# contributors" /></a>
<a href="https://zenodo.org/badge/latestdoi/104919828">
<img src="https://zenodo.org/badge/104919828.svg"
 alt="zenodo" /></a>
<a href="https://pypi.org/project/erddapy">
<img src="https://img.shields.io/pypi/pyversions/erddapy.svg"
 alt="zenodo" /></a>
<a href="https://results.pre-commit.ci/latest/github/ioos/erddapy/main">
<img src="https://results.pre-commit.ci/badge/github/ioos/erddapy/main.svg"
 alt="pre-commit.ci status" /></a>

 <a href="https://github.com/ioos/erddapy/actions">
<img src="https://github.com/ioos/erddapy/actions/workflows/tests.yml/badge.svg"
 alt="GHA-tests" /></a>


</p>

<br>



# Table of contents

<!-- toc -->

- [Overview](#overview)
  - [Example](#example)
- [Get in touch](#get-in-touch)
- [License and copyright](#license-and-copyright)

<!-- tocstop -->


## Overview

Easier access to scientific data.

erddapy takes advantage of ERDDAP's RESTful web services and creates the ERDDAP URL for any request,
like searching for datasets, acquiring metadata, downloading the data, etc.

What is ERDDAP?
ERDDAP unifies the different types of data servers and offers a consistent way to get the data in multiple the formats.
For more information on ERDDAP servers please see [https://coastwatch.pfeg.noaa.gov/erddap/index.html](https://coastwatch.pfeg.noaa.gov/erddap/index.html).

### Documentation and code

The documentation is hosted at <https://ioos.github.io/erddapy>.

The code is hosted at <https://github.com/ioos/erddapy>.

### Installation

For `conda` users you can

```shell
conda install --channel conda-forge erddapy
```

or, if you are a `pip` users

```shell
pip install erddapy
```

Note that, if you are installing the `requirements-dev.txt`, the `iris` package
is named `scitools-iris` on PyPI so `pip` users must rename that before installing.

### Example

```python
from erddapy import ERDDAP


e = ERDDAP(
  server="https://gliders.ioos.us/erddap",
  protocol="tabledap",
)

e.response = "csv"
e.dataset_id = "whoi_406-20160902T1700"
e.constraints = {
    "time>=": "2016-07-10T00:00:00Z",
    "time<=": "2017-02-10T00:00:00Z",
    "latitude>=": 38.0,
    "latitude<=": 41.0,
    "longitude>=": -72.0,
    "longitude<=": -69.0,
}
e.variables = [
    "depth",
    "latitude",
    "longitude",
    "salinity",
    "temperature",
    "time",
]

df = e.to_pandas()
```


## Get in touch

Report bugs, suggest features or view the source code on [GitHub](https://github.com/ioos/erddapy/issues).

## Projects using erddapy

- [argopy](https://github.com/euroargodev/argopy)
- [gliderpy](https://github.com/ioos/gliderpy)
- [gdutils](https://github.com/kerfoot/gdutils)
- [colocate](https://github.com/ioos/colocate)
- [intake-erddap](https://github.com/jmunroe/intake-erddap)
- [ioos_qc](https://github.com/ioos/ioos_qc)

## Similar projects

- [rerddap](https://cran.r-project.org/web/packages/rerddap) implements a nice client for R that performs searches on a curated set of servers instead of a query per server like erddapy.

- [erddap-python](https://github.com/hmedrano/erddap-python) 99% of the same functionality as erddapy but with a JavaScript-like API.

## License and copyright

Erddapy is licensed under BSD 3-Clause "New" or "Revised" License (BSD-3-Clause).

Development occurs on GitHub at <https://github.com/ioos/erddapy>.
