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
    #bgnyear = int(bgndte[0:4])
    #endyear = int(enddte[0:4])
    for datayear in range(int(bgndte[0:4]), int(enddte[0:4])+1):
        yearlastday = str(datayear) + '-12-31'
        yearfirstday = str(datayear) + '-01-01'
        if enddte >= yearlastday:
            endday = yearlastday
        else:
            endday = enddte
        if bgndte <= yearfirstday:
            bgnday = yearfirstday
        else:
            bgnday = bgndte
        df = ts.get_h_data(code=stockcode, start=bgnday, end=endday, retry_count=100, pause=5)
        print(stockcode,bgnday,endday,len(df.index))
        df.to_sql('tmpdata', engine, if_exists='replace')
        cursor.execute(
            'delete from stockdaydata where ts_code = \'%s\' and ts_date between \'%s\' and \'%s\''
            % (stockcode, bgnday, endday))
        cursor.execute("insert into stockdaydata select \'%s\' , a.* from tmpdata a " % (stockcode))
        conn.commit()
        datayear += datayear

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
    #print(record)






#loaddaydata('600036')
