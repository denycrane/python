# -*- coding = utf-8 -*-
from sqlalchemy import create_engine
import tushare as ts
import mysql.connector
import datetime

def loaddaydata(stockcode, bgndte, enddte):
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    engine = create_engine('mysql://root:ms@ciji1995@127.0.0.1/tushare?charset=utf8')
    print(stockcode, bgndte, enddte)

    df = ts.get_h_data(code=stockcode, start=bgndte, end=enddte)
    print(df)
    df.to_sql('tmpdata', engine, if_exists='replace')
    cursor.execute(
        'delete from stockdaydata where ts_code = \'%s\' and ts_date between \'%s\' and \'%s\''
        % (stockcode, bgndte, enddte))
    cursor.execute("insert into stockdaydata select \'%s\' , a.* from tmpdata a " % (stockcode))
    conn.commit()
    cursor.close()
    conn.close()
    return df

def loadstockbasics():
    conn = mysql.connector.connect(user='root',password='ms@ciji1995',database='tushare')
    cursor = conn.cursor()
    engine = create_engine('mysql://root:ms@ciji1995@127.0.0.1/tushare?charset=utf8')
    df = ts.get_stock_basics()
    cursor.execute('delete from stockbasics')
    conn.commit()
    df.to_sql('stockbasics', engine, if_exists='append')
    cursor.close()
    conn.close()
    return df

def loadalldaydata(code='ALL'):
    sb = loadstockbasics()
    today = datetime.date.today()
    stockenddte = today.strftime('%Y-%m-%d')
    #print(stockenddte)
    record ={}
    codelist = [code]
    if code == 'ALL':
        codelist = sb.index
    for stockcode in codelist:
        bgnpre = str(sb['timeToMarket'][stockcode])
        if len(bgnpre) == 8:
            stockbgndte = datetime.datetime.strptime(str(bgnpre),"%Y%m%d").strftime('%Y-%m-%d')
            rs = loaddaydata(stockcode,stockbgndte,stockenddte)
            record[stockcode]=len(rs.index)
    print(record)






#loaddaydata('600036')
