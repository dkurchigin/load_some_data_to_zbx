from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
import time
import datetime

user = "zabbix"
passwd = "..."
server_db = "..."
db = "zabbix"

CONNECTION_STRING = f'postgresql+psycopg2://{user}:{passwd}@{server_db}/{db}'
engine = create_engine(CONNECTION_STRING, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class ZBXHistoryUint(Base):
    __tablename__ = 'history_uint'

    itemid = Column(Integer, primary_key=True)
    clock = Column(Integer)
    value = Column(Integer)
    ns = Column(Integer)

    def __repr__(self):
        return f"<Itemid - {self.itemid}>"


def push_data(row):
    wr = ZBXHistoryUint()
    rd = ZBXHistoryUint()

    wr.itemid = 28638
    rd.itemid = 28637

    ts = int(time.mktime(datetime.datetime.strptime(row[0], "%d.%m.%Y %H:%M:%S").timetuple()))

    wr.clock = ts
    rd.clock = ts

    wr.value = row[2]
    rd.value = row[1]

    wr.ns = 0
    rd.ns = 0

    session.add(wr)
    session.add(rd)
    print(ts)


with open('sxdwriter_2020-08-06_00-00-00.csv', newline='') as csvfile:
    csv_ = csv.reader(csvfile, delimiter=',')
    i = 0
    for row in csv_:
        i = i + 1
        if row[0] == 'Time':
            continue
        push_data(row)
        if i % 4000 == 0:
            session.commit()
