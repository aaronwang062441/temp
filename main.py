#!/usr/bin/env python
# encoding: utf-8
# author: wangminghui

import os
import csv
import sys
from datetime import datetime

sys.path.append(os.path.dirname(__file__))
from database import DB_IMPALA, MysqlDB
from config import IMPALA_CONFIG

impala_database = DB_IMPALA(IMPALA_CONFIG, 'impala_zilong')
fmt_sql = "SELECT carno, userid, endtime FROM t_order5 WHERE carno = '{carno}' \
    AND createtime <= '{time}' AND endtime >= '{time}' order by createtime DESC limit 1"


def process_csv(path):
    if os.path.isfile(path):
        with open("result.csv","w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["time","carno","userid"])

        with open(path, "r") as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                carno, time_str = line[1], datetime.strptime(line[0], "%Y/%m/%d %H:%M").strftime("%Y-%m-%d %H:%M:00")
                sql = fmt_sql.format(carno=carno, time=time_str)
                print(sql)
                data = impala_database.execute(sql)
                if not data:
                    res = " "
                    with open("result.csv","a") as csvfile:
                        writer = csv.writer(csvfile, dialect="excel")
                        writer.writerow([time_str, carno, res])
                    continue

                res_list = list()
                for item in data:
                    """
                    {'carno': '3072951', 'endtime': '2017-09-23 14:58:31', 'userid': 150104479}
                    {'carno': '3072951', 'endtime': 'null', 'userid': 111755504}
                    {'carno': '3072951', 'endtime': 'null', 'userid': 66265893}
                    {'carno': '3072951', 'endtime': 'null', 'userid': 14842141}
                    {'carno': '3072951', 'endtime': 'null', 'userid': 23213541}

                    """
                    print(item)
                    if item['endtime'] != 'null':
                        res_list.append(str(item['userid']))
                print(res_list)
                if res_list:
                    res = ",".join(res_list)
                else:
                    res = " "

                with open("result.csv","a") as csvfile:
                    writer = csv.writer(csvfile, dialect="excel")
                    writer.writerow([time_str, carno, res])

            print("finish")



if __name__ == "__main__":
    process_csv("first.csv")
