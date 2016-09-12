create table stockdaydata(
    ts_code   varchar(10) not null,
    ts_date   date not null,
    ts_open   double ,
    ts_high   double,
    ts_close  double,
    ts_low    double,
    ts_volume double,
    ts_amount double,
    primary key(ts_code, ts_date)
    )
    
    