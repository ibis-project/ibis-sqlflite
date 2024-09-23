# ibis-sqlflite
A reverse proxy server which allows secure connectivity to a Spark Connect server.

[<img src="https://img.shields.io/badge/GitHub-prmoore77%2Fspark--connect--proxy-blue.svg?logo=Github">](https://github.com/prmoore77/ibis-sqlflite)
[![ibis-sqlflite-ci](https://github.com/prmoore77/ibis-sqlflite/actions/workflows/ci.yml/badge.svg)](https://github.com/prmoore77/ibis-sqlflite/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/ibis-sqlflite)](https://pypi.org/project/ibis-sqlflite/)
[![PyPI version](https://badge.fury.io/py/ibis-sqlflite.svg)](https://badge.fury.io/py/ibis-sqlflite)
[![PyPI Downloads](https://img.shields.io/pypi/dm/ibis-sqlflite.svg)](https://pypi.org/project/ibis-sqlflite/)

# Why?
Because [Spark Connect does NOT provide authentication and/or TLS encryption out of the box](https://spark.apache.org/docs/latest/spark-connect-overview.html#client-application-authentication).  This project provides a reverse proxy server which can be used to secure the connection to a Spark Connect server.

# Setup (to run locally)

## Install Python package
You can install `ibis-sqlflite` from PyPi or from source.

### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install ibis-sqlflite
```

### Option 2 - from source - for development
```shell
git clone https://github.com/prmoore77/ibis-sqlflite

cd ibis-sqlflite

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install Spark Connect Proxy - in editable mode with client and dev dependencies
pip install --editable .[test]
```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/src
```
