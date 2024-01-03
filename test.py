from influxdb2_dbapi import connect

conn = connect(host='10.20.4.53', port=30485, org='ulak', token="KbSyJTKzIuvxqpKjVMTautakGg7uPxGTF3hz878Ye4CH_UgTl0k4W2UXYy79dwrzSle9QmEt2KCde0Sf88qhSQ==", scheme='http')

curs = conn.cursor()
# curs.execute("""
#
#     from(bucket: "collectd")
#       |> range(start: -1m, stop: 1m)
#       |> filter(fn: (r) => r["_measurement"] == "cpu")
#       |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
#       |> yield(name: "mean")
# """)
# # or
# curs.execute("""
#     SELECT  *
#     FROM (
#
#     from(bucket: "collectd")
#       |> range(start: -1m, stop: 1m)
#       |> filter(fn: (r) => r["_measurement"] == "cpu")
#       |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
#       |> yield(name: "mean")
#
#     )as qry
#      LIMIT 10
# """)
# # or
# curs.execute("""
#      from(bucket: "collectd")
#       |> range(start: -1m, stop: 1m)
#       |> filter(fn: (r) => r["_measurement"] == "cpu")
#       |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
#       |> yield(name: "mean")
# """)
# or
curs.execute("""
    SELECT  * 
    FROM (

     from(bucket: "collectd")
      |> range(start: -1m, stop: 1m)
      |> filter(fn: (r) => r["_measurement"] == "cpu")
      |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
      |> yield(name: "mean")

    )as qry
     LIMIT 10
""")
for row in curs:
    print(row._fields)
    print(row.values)
