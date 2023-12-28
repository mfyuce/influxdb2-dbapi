# A Python DB API 2.0 for InfluxDb2 with SQLAlchemy (Draft)

This module allows accessing influxdb2 with SQLAlchemy.

## Usage ##

Using the DB API:

```python
from influxdb2_dbapi import connect

conn = connect(host='localhost', port=8086,scheme='http',org=..,token=..)
curs = conn.cursor()
curs.execute("""
    from(bucket: "...")
     ...
""")
#or 
curs.execute("""
    SELECT  * 
    FROM (
        
    
    from(bucket: "...")
     ...

    )as qry
     LIMIT 10
""")
#or
curs.execute(""" 
    from(bucket: "...")
    ...
""")
#or
curs.execute("""
    SELECT  * 
    FROM (
        from(bucket: "...")
        ...
    )as qry
     LIMIT 10
""")
for row in curs:
    print(row)
```

Using SQLAlchemy:

```python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('influxdb2://localhost:8086')  # uses HTTP by default :( 

places = Table('places', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=places).scalar())
```

Using the REPL:

```bash
$ influxdb2_db http://localhost:8086/
>  from(bucket:...)
  cnt
-----
12345
```


# Local install

```bash
python -m pip install -e  .
```
