#!/bin/ksh

summary()
{
${SQLPLUS} -s dash/dash123@elixirdb << EOFEOF
set pages 0
set lines 1000
set trimspool on
set feedback off

select substr(RNC,1,3)||' '|| RNC||' '||count (*) as TOTAL from (
select CELLNAME,CELLID,RNC,NODEB_NAME, date1, date2, date3, date4, date5, date6, date7,avg,
(cat1 + cat2 + cat3 + cat4 + cat5 + cat6 + cat7) as status,
case when cat1 + cat2 + cat3 + cat4 + cat5 + cat6 + cat7 >= 4 then 'Consistent High Mean RTWP' END as remarks
from (select CELLNAME,CELLID,RNC,NODEB_NAME, date1, date2, date3, date4, date5, date6, date7, round(sum(date1 + date2 + date3 + date4 + date5 + date6 + date7)/7,3) as avg,
case when date1 > -90 then '1' END as cat1,
case when date2 > -90 then '1' END as cat2,
case when date3 > -90 then '1' END as cat3,
case when date4 > -90 then '1' END as cat4,
case when date5 > -90 then '1' END as cat5,
case when date6 > -90 then '1' END as cat6,
case when date7 > -90 then '1' END as cat7
from (select CELLNAME,CELLID,RNC,NODEB_NAME,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-6 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date1,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-5 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date2,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-4 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date3,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-3 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date4,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-2 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date5,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-1 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date6,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-0 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date7
from PS_ACCESS_CELL
where to_char(timestamp,'hh24') = '04'
group by CELLNAME,CELLID,RNC,NODEB_NAME)
group by CELLNAME,CELLID,RNC,NODEB_NAME, date1, date2, date3, date4, date5, date6, date7))
where remarks is not null
group by substr(RNC,1,3),RNC
order by 1;


exit;

EOFEOF
}


extract()
{
${SQLPLUS} -s dash/dash123@elixirdb << EOFEOF
set pages 0
set lines 1000
set trimspool on

select 'CELLNAME,CELLID,RNC,NODEB NAME,'||(select trunc(max(timestamp))-6 from PS_ACCESS_CELL)||','||
(select trunc(max(timestamp))-5 from PS_ACCESS_CELL)||','||
(select trunc(max(timestamp))-4 from PS_ACCESS_CELL)||','||
(select trunc(max(timestamp))-3 from PS_ACCESS_CELL)||','||
(select trunc(max(timestamp))-2 from PS_ACCESS_CELL)||','||
(select trunc(max(timestamp))-1 from PS_ACCESS_CELL)||','||
(select trunc(max(timestamp))-0 from PS_ACCESS_CELL)||',AVERAGE,CONSISTENCY COUNT,REMARKS' from dual;


select CELLNAME||','||CELLID||','||RNC||','||NODEB_NAME||','||date1||','||date2||','||date3||','||date4||','||date5||','||date6||','||date7||','||avg||','||
(cat1 + cat2 + cat3 + cat4 + cat5 + cat6 + cat7)||','||
case when cat1 + cat2 + cat3 + cat4 + cat5 + cat6 + cat7 >= 4 then 'Consistent High Mean RTWP' END
from (select CELLNAME,CELLID,RNC,NODEB_NAME, date1, date2, date3, date4, date5, date6, date7, round(sum(date1 + date2 + date3 + date4 + date5 + date6 + date7)/7,3) as avg,
case when date1 > -90 then '1' else '0' END as cat1,
case when date2 > -90 then '1' else '0' END as cat2,
case when date3 > -90 then '1' else '0' END as cat3,
case when date4 > -90 then '1' else '0' END as cat4,
case when date5 > -90 then '1' else '0' END as cat5,
case when date6 > -90 then '1' else '0' END as cat6,
case when date7 > -90 then '1' else '0' END as cat7
from (select CELLNAME,CELLID,RNC,NODEB_NAME,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-6 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date1,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-5 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date2,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-4 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date3,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-3 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date4,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-2 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date5,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-1 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date6,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-0 from PS_ACCESS_NODEB),nullif(MEAN_RTWP,0),NULL)) as date7
from PS_ACCESS_CELL
where to_char(timestamp,'hh24') = '04'
group by CELLNAME,CELLID,RNC,NODEB_NAME)
group by CELLNAME,CELLID,RNC,NODEB_NAME, date1, date2, date3, date4, date5, date6, date7);

exit;

EOFEOF
}


weekno()
{
sqlplus -s dash/dash123@elixirdb << EOFEOF
set pages 0
set lines 1000
set trimspool on

select 'WEEKNO:'||to_char(sysdate-6,'YYYY-IW') from dual;
exit;

EOFEOF
}


#MAIN
PROFILE=/export/home/oracle/.profile
. $PROFILE

BASE_DIR=/apps/DASHBOARD/REPORTS
CONFIG=${BASE_DIR}/config
BIN=${BASE_DIR}/bin
ALARM=${BASE_DIR}/alarm
LOG=${BASE_DIR}/logs
DATA=${BASE_DIR}/data
SQLLDR=/export/home/oracle/product/11g/bin/sqlldr
SQLPLUS=/export/home/oracle/product/11g/bin/sqlplus
ouser=dash
opass=dash123

BODY=${BASE_DIR}/config/mean_rtwp_body.txt
EMAIL_LIST=${BASE_DIR}/config/mean_rtwp_list.txt
SENDER=elixir@globe.com.ph
HEADER=${BASE_DIR}/config/mean_rtwp_header.cfg
TRAILER=${BASE_DIR}/config/mean_rtwp_trailer.cfg
REPORT=${BASE_DIR}/REPORT_FILES/PS
deyt=`date "+%Y%m%d"`
weekno=`weekno | grep WEEKNO | cut -f 2 -d :`
SUBJECT=`echo "Mean RTWP Report for week ${weekno}"`
ATTACH=${REPORT}/mean_rtwp_report_${weekno}.csv.gz

echo "`date` Processing..."
extract | grep , > ${REPORT}/mean_rtwp_report_${weekno}.csv

#COMPOSE EMAIL BODY
cat $HEADER > ${BODY}
echo "REGION&emsp;&emsp;&emsp;RNC&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;TOTAL" >> ${BODY}
summary | sed -e 's/ /\&emsp;\&emsp;\&emsp;/g' >> ${BODY}
cat $TRAILER >> ${BODY}

gzip -f ${REPORT}/mean_rtwp_report_${weekno}.csv

#EMAIL DATA

#DEBUG
#echo "/apps/EMAIL/bin/email.ksh ${SENDER} ${EMAIL_LIST} ${BODY} "${SUBJECT}" ${ATTACH}"


/apps/EMAIL/bin/email.ksh ${SENDER} ${EMAIL_LIST} ${BODY} "${SUBJECT}" ${ATTACH}

echo "`date` Done..."

#END PROGRAM
