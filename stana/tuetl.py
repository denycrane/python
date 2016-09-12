# -*- coding = utf-8 -*-
from sqlalchemy import create_engine
import tushare as ts
import mysql.connector
import datetime


def loaddaydata(stockcode, bgndte, enddte, cflg='N', loadmode='YEAR'):
    print('\n\n--', datetime.datetime.now(), '\nloaddaydata:', stockcode, bgndte, enddte, )
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    engine = create_engine('mysql://root:ms@ciji1995@127.0.0.1/tushare?charset=utf8')
    cursor.execute("select * from stockdaydata where ts_code = '%s' order by ts_date desc limit 1" % (stockcode))
    dbrs = cursor.fetchall()
    conn.commit()
    if dbrs != [] and cflg=='Y':
        dbdate = dbrs[0][1]
        dblastdate = str(dbdate)
        dbrecord = dbrs[0][2:]
        df1 = ts.get_h_data(code=stockcode, start=dblastdate, end=dblastdate)
        turecord = tuple(df1.ix[dblastdate])
        #print('dbrecord: ', dbrecord)
        #print('turecord: ', turecord)
        #print('bgndte:   ', bgndte)
        if dbrecord == turecord and bgndte < dblastdate:
            bgndte = str(dbdate + datetime.timedelta(1))
            print('change bgndte to ', bgndte)
        if enddte <= bgndte:
            cursor.close()
            conn.close()
            print('record is already!')
            return
    # bgnyear = int(bgndte[0:4])
    # endyear = int(enddte[0:4])
    print(stockcode, bgndte, enddte)
    mntcnt = 0
    if loadmode == 'ONCE':
        df = ts.get_h_data(code=stockcode, start=bgndte, end=enddte, retry_count=100, pause=5)
        if df is None:
            return
        df.to_sql('tmpdata', engine, if_exists='replace')
        mntcnt = len(df.index)
        cursor.execute(
            "delete from stockdaydata where ts_code = '%s' and ts_date between '%s' and '%s'"
            % (stockcode, bgndte, enddte))
        cursor.execute("insert into stockdaydata select '%s' , a.* from tmpdata a " % (stockcode))
        conn.commit()
    elif loadmode == 'YEAR':
        for datayear in range(int(bgndte[0:4]), int(enddte[0:4]) + 1):
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
            if df is None:
                continue
            recnt = len(df.index)
            print(stockcode, bgnday, endday, recnt)
            mntcnt += recnt
            df.to_sql('tmpdata', engine, if_exists='replace')
            cursor.execute(
                "delete from stockdaydata where ts_code = '%s' and ts_date between '%s' and '%s'"
                % (stockcode, bgnday, endday))
            cursor.execute("insert into stockdaydata select '%s' , a.* from tmpdata a " % (stockcode))
            conn.commit()
            datayear += datayear

    conn.commit()
    cursor.close()
    conn.close()
    return 'record number is :', mntcnt


def loadstockbasics():
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
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
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    cursor.execute("select distinct ts_code from stockdaydata")
    dbcodelist = cursor.fetchall()
    sb = loadstockbasics()
    today = datetime.date.today()
    stockenddte = today.strftime('%Y-%m-%d')
    # print(stockenddte)
    #record = {}
    codelist = [code]
    if code == 'ALL':
        codelist = sb.index
    elif code == 'ADD':
        codelist = sb.index - dbcodelist
    for stockcode in codelist:
        bgnpre = str(sb['timeToMarket'][stockcode])
        if len(bgnpre) == 8:
            stockbgndte = datetime.datetime.strptime(str(bgnpre), "%Y%m%d").strftime('%Y-%m-%d')
            loaddaydata(stockcode, stockbgndte, stockenddte, cflg='Y')
            #record[stockcode] = len(rs.index)
            # print(record)

# loaddaydata('600036')
