# A Python DB API 2.0 for OLAP #

This module allows accessing OLAP via its [XMLA](https://pypi.org/project/netas-xmla-with-dax/) using MDX or DAX.

## Usage ##

Using the DB API:

```python
from netas_olap_dbapi import connect

conn = connect(host='localhost', port=80, path='/OLAP/msmdpump.dll', scheme='http')
curs = conn.cursor()
curs.execute("""
    EVALUATE SUMMARIZECOLUMNS(..., FILTER(VALUES(...), ...)
""")
#or 
curs.execute("""
    SELECT  * 
    FROM (
        
    EVALUATE SUMMARIZECOLUMNS(..., FILTER(VALUES(...), ...)

    )as qry
     LIMIT 10
""")
#or
curs.execute("""
    SELECT [Dim].[Fld].[(All)] on 0,
        [Dim2].[Fld2].[(All)] on 1
   FROM [MODEL]
""")
#or
curs.execute("""
    SELECT  * 
    FROM (
        
    SELECT [Dim].[Fld].[(All)] on 0,
        [Dim2].[Fld2].[(All)] on 1,
        [Dim3].[Fld3].[(All)] on 2
   FROM [MODEL]

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

engine = create_engine('ssas://localhost:80/OLAP/msmdpump.dll')  # uses HTTP by default :(
# engine = create_engine('ssas+http://localhost:8082/OLAP/msmdpump.dll')
# engine = create_engine('ssas+https://localhost:8082/OLAP/msmdpump.dll')

places = Table('places', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=places).scalar())
```

Using the REPL:

```bash
$ netas_olap_db http://localhost:80/OLAP/msmdpump.dll
> EVALUATE SUMMARIZECOLUMNS(..., FILTER(VALUES(...), ...)
  cnt
-----
12345
```
