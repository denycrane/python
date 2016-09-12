#drop table tmpbascis;
CREATE TABLE `stockbasics` (
  `code` varchar(10) not null primary key,
  `name` text,
  `industry` text,
  `area` text,
  `pe` double DEFAULT NULL,
  `outstanding` double DEFAULT NULL,
  `totals` double DEFAULT NULL,
  `totalAssets` double DEFAULT NULL,
  `liquidAssets` double DEFAULT NULL,
  `fixedAssets` double DEFAULT NULL,
  `reserved` double DEFAULT NULL,
  `reservedPerShare` double DEFAULT NULL,
  `esp` double DEFAULT NULL,
  `bvps` double DEFAULT NULL,
  `pb` double DEFAULT NULL,
  `timeToMarket` bigint(20) DEFAULT NULL
)