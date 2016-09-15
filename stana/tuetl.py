# -*- coding = utf-8 -*-
from sqlalchemy import create_engine
import tushare as ts
import mysql.connector
import datetime
import threading
from multiprocessing import Pool
import math
import os


def loaddaydata(stockcode, bgndte, enddte, addflg='N',loadmode='ONCE'):
    print('\n\n--', datetime.datetime.now(), '\nloaddaydata:', stockcode, bgndte, enddte, )
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    engine = create_engine('mysql://root:ms@ciji1995@127.0.0.1/tushare?charset=utf8')
    cursor.execute("select * from stockdaydata where ts_code = '%s' order by ts_date desc limit 1" % (stockcode))
    dbrs = cursor.fetchall()
    conn.commit()
    tablename = 'tmp'+stockcode;
    #从现有的日期后追加数据，修改开始及结束日期
    if dbrs != [] and addflg == 'Y':
        dbdate = dbrs[0][1]
        dblastdate = str(dbdate)
        dbrecord = dbrs[0][2:]
        df1 = ts.get_h_data(code=stockcode, start=dblastdate, end=dblastdate)
        #print(df1)
        turecord = tuple(df1.ix[dblastdate])
        #print('dbrecord: ', dbrecord)
        #print('turecord: ', turecord)
        #print('bgndte:   ', bgndte)
        #print('dbdate:', dbdate)
        print(bgndte,enddte)
        if dbrecord == turecord and bgndte < dblastdate:
            bgnpre = dbdate + datetime.timedelta(1)
            weekday = datetime.datetime.weekday(bgnpre)
            #print(bgnpre,weekday)
            if weekday == 5:
                bgnpre += datetime.timedelta(2)
            elif weekday == 6:
                bgnpre += datetime.timedelta(1)
            bgndte = str(bgnpre)
            print('change bgndte to ', bgndte)
        if enddte < bgndte:
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
            print('no record in this perio ')
            return
        df.to_sql(tablename, engine, if_exists='replace')
        mntcnt = len(df.index)
        cursor.execute(
            "delete from stockdaydata where ts_code = '%s' and ts_date between '%s' and '%s'"
            % (stockcode, bgndte, enddte))
        cursor.execute("insert into stockdaydata select '%s' , a.* from %s a " % (stockcode, tablename))
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
    print('record number is :', mntcnt)
    return


def loadstockbasics():
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    engine = create_engine('mysql://root:ms@ciji1995@127.0.0.1/tushare?charset=utf8')
    print('start...')
    df = ts.get_stock_basics()
    print(df)
    cursor.execute("delete from stockbasics")
    conn.commit()
    print('delete stockbasics ok!')
    df.to_sql('stockbasics', engine, if_exists='append')
    conn.commit()
    print('insert stockbasics ok!')
    cursor.close()
    conn.close()
    return


def loadlistdaydata(codelist, aflg='Y', lmodel='ONCE'):
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    cursor.execute("select code ,date(timeToMarket) from stockbasics")
    dbcodelist = cursor.fetchall()
    sb = dict(dbcodelist)
    today = datetime.date.today()
    stockenddte = today.strftime('%Y-%m-%d')
    # print(stockenddte)
    #record = {}print('dbdate:', dbdate)
    for stockcode in codelist:
        stockbgndte = sb[stockcode].strftime('%Y-%m-%d')
        loaddaydata(stockcode, stockbgndte, stockenddte, addflg=aflg,loadmode=lmodel)
            #record[stockcode] = len(rs.index)
            # print(record)

def loadalldaydata(threadcnt=30,mode='ALL'):
    conn = mysql.connector.connect(user='root', password='ms@ciji1995', database='tushare')
    cursor = conn.cursor()
    cursor.execute("select code from stockbasics order by code")
    dbbasicscode = cursor.fetchall()
    conn.commit()
    cursor.execute("select ts_code from stockdaydata group by ts_code order by ts_code")
    dbdaydatacode = cursor.fetchall()
    conn.commit()
    if mode == 'ADD':
        dbcoderesult = list(set(dbbasicscode) - set(dbdaydatacode))
    elif mode == 'ALL':
        dbcoderesult = dbbasicscode
    dbcodelist = [dbcoderesult[x][0] for x in range(len(dbcoderesult))]
    codecnt = len(dbcodelist)
    threadcodecnt = math.ceil(codecnt/threadcnt)
    startid = 0
    p = Pool(threadcnt)
    endid = startid + threadcodecnt
    codelist =  [[0 for col in range(threadcodecnt)] for row in range(threadcnt)]
    for threadid in range(threadcnt):
        codelist[threadid] = dbcodelist[startid:endid]
        print('\n\n',codelist[threadid])
        startid += threadcodecnt
        endid += threadcodecnt
        if endid > codecnt + 1:
            endid = codecnt + 1
    #return codelist
    threadlist = []
    for threadid in range(threadcnt):
        sthread=threading.Thread(target=loadlistdaydata,args=(codelist[threadid],))
        sthread.setDaemon(True)
        sthread.start()
        threadlist.append(sthread)
    for threadid in range(threadcnt):
        threadlist[threadid].join()