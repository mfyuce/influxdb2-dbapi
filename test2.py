from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('influxdb2://10.20.4.53:30485/collectd?org=ulak&token=KbSyJTKzIuvxqpKjVMTautakGg7uPxGTF3hz878Ye4CH_UgTl0k4W2UXYy79dwrzSle9QmEt2KCde0Sf88qhSQ==')  # uses HTTP by default :(


places = Table('places', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=places).scalar())