# ibis-sqlflite
An [Ibis](https://ibis-project.org) back-end for [SQLFlite](https://github.com/voltrondata/sqlflite)

[<img src="https://img.shields.io/badge/GitHub-prmoore77%2Fibis--sqlflite-blue.svg?logo=Github">](https://github.com/prmoore77/ibis-sqlflite)
[![ibis-sqlflite-ci](https://github.com/prmoore77/ibis-sqlflite/actions/workflows/ci.yml/badge.svg)](https://github.com/prmoore77/ibis-sqlflite/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/ibis-sqlflite)](https://pypi.org/project/ibis-sqlflite/)
[![PyPI version](https://badge.fury.io/py/ibis-sqlflite.svg)](https://badge.fury.io/py/ibis-sqlflite)
[![PyPI Downloads](https://img.shields.io/pypi/dm/ibis-sqlflite.svg)](https://pypi.org/project/ibis-sqlflite/)

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
pip install --editable .[dev,test]
```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/ibis_sqlflite
```

### Usage
In this example - we'll start a SQLFlite server with the DuckDB back-end in Docker, and connect to it from Python using Ibis.

First - start the SQLFlite server - which by default mounts a small TPC-H database:
```shell
docker run --name sqlflite \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env SQLFLITE_PASSWORD="sqlflite_password" \
           --env PRINT_QUERIES="1" \
           --pull missing \
           voltrondata/sqlflite:latest
```

Next - connect to the SQLFlite server from Python using Ibis by running this Python code:
```python
import os
import ibis
from ibis import _

# Kwarg connection example
con = ibis.sqlflite.connect(host="localhost",
                            user=os.getenv("SQLFLITE_USERNAME", "sqlflite_username"),
                            password=os.getenv("SQLFLITE_PASSWORD", "sqlflite_password"),
                            port=31337,
                            use_encryption=True,
                            disable_certificate_verification=True
                            )

# URL connection example
# con = ibis.connect("sqlflite://sqlflite_username:sqlflite_password@localhost:31337?disableCertificateVerification=True&useEncryption=True")

print(con.tables)

# assign the LINEITEM table to variable t (an Ibis table object)
t = con.table('lineitem')

# use the Ibis dataframe API to run TPC-H query 1
results = (t.filter(_.l_shipdate.cast('date') <= ibis.date('1998-12-01') + ibis.interval(days=90))
       .mutate(discount_price=_.l_extendedprice * (1 - _.l_discount))
       .mutate(charge=_.discount_price * (1 + _.l_tax))
       .group_by([_.l_returnflag,
                  _.l_linestatus
                  ]
                 )
       .aggregate(
            sum_qty=_.l_quantity.sum(),
            sum_base_price=_.l_extendedprice.sum(),
            sum_disc_price=_.discount_price.sum(),
            sum_charge=_.charge.sum(),
            avg_qty=_.l_quantity.mean(),
            avg_price=_.l_extendedprice.mean(),
            avg_disc=_.l_discount.mean(),
            count_order=_.count()
        )
       .order_by([_.l_returnflag,
                  _.l_linestatus
                  ]
                 )
       )

print(results.execute())
```

You should see output:
```text
  l_returnflag l_linestatus    sum_qty sum_base_price sum_disc_price     sum_charge avg_qty avg_price avg_disc  count_order
0            A            F  380456.00   532348211.65   505822441.49   526165934.00   25.58  35785.71     0.05        14876
1            N            F    8971.00    12384801.37    11798257.21    12282485.06   25.78  35588.51     0.05          348
2            N            O  765251.00  1072862302.10  1019517788.99  1060424708.62   25.47  35703.76     0.05        30049
3            R            F  381449.00   534594445.35   507996454.41   528524219.36   25.60  35874.01     0.05        14902
```

### Handy development commands

#### Version management

##### Bump the version of the application - (you must have installed from source with the [dev] extras)
```bash
bumpver update --patch
```
