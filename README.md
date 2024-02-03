[![flake8 Lint](https://github.com/acdh-oeaw/acdh-baserow-pyutils/actions/workflows/lint.yml/badge.svg)](https://github.com/acdh-oeaw/acdh-baserow-pyutils/actions/workflows/lint.yml)
[![Test](https://github.com/acdh-oeaw/acdh-baserow-pyutils/actions/workflows/test.yml/badge.svg)](https://github.com/acdh-oeaw/acdh-baserow-pyutils/actions/workflows/test.yml)
[![codecov](https://codecov.io/github/acdh-oeaw/acdh-baserow-pyutils/branch/main/graph/badge.svg?token=8B1K7Y36HN)](https://codecov.io/github/acdh-oeaw/acdh-baserow-pyutils)
[![PyPI version](https://badge.fury.io/py/acdh-baserow-pyutils.svg)](https://badge.fury.io/py/acdh-baserow-pyutils)

# acdh-baserow-pyutils
a python client for baserow

## install

`pip install acdh-baserow-pyutils`


## how to use

Have a look into `tests/test_baserow_client.py`

### dump all tables of a given database into JSON-FILES

```python
import os
from acdh_baserow_utils import BaseRowClient

# store baserow credentials as ENV-Variables
BASEROW_USER = os.environ.get("BASEROW_USER")
BASEROW_PW = os.environ.get("BASEROW_PW")
BASEROW_TOKEN = os.environ.get("BASEROW_TOKEN") # you need to create a token via baserow
DATABASE_ID = "41426" # you can get this ID from Baserow

# initialize the client
br_client = BaseRowClient(BASEROW_USER, BASEROW_PW, BASEROW_TOKEN)

# writes all tables from Database as json.files into a folder 'out' (the folder needs to exist!) and returns a list of the file names
files = br_client.dump_tables_as_json(DATABASE_ID, folder_name='out')
print(files)
# ['out/place.json', 'out/person.json', 'out/profession.json']
```

