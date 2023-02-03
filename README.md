# About

Export IRIS data to several excel files.

# Pre-requirements

Create a virtual environment, activate it and install the `requirements.txt`:

 - `$ python -m venv env`
 - `$ source env/bin/activate`
 - `$ pip install -r requirements.txt`

# Run

Export with original field requires the indice to have a field "Original". And uses a specific map.

`python export-with-original.py <index-name> [-e <field-to-exclude>] [-i <field-id>] [-o <output-folder>]`

# Structure for each `<field>.xlsx` and `<field>-aggs.xlsx`

| ID | Original | Atual | Correção |
|----|----------|-------|----------|
| Value of `<field-id>` | Value of `_source.Original.<field>` | Value of `_source.<field>` | Empty |

| Count | Atual | Correção |
|-------|-------|----------|
| `doc_count` of aggregation of `_source.<field>[.raw\|.keyword]` by terms | `key\|key_as_string` of said aggregation  | Empty |





