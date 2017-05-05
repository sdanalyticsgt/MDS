#!/bin/ksh

summary()
{
${SQLPLUS} -s dash/dash123@elixirdb << EOFEOF
set pages 0
set lines 300
set trimspool on
--break on CATEGORY
--break on CATEGORY skip page
col CATEGORY format a80
col REGION format a20
col RNC format a20
set feedback off


select remarks||','||substr(RNC,1,3)||','||RNC||','||count (*) from (
select RNC,NODEB_NAME,NODEBID,date1,date2,date3,date4,date5,date6,date7,avg,
(cata1 + cata2 + cata3 + cata4 + cata5 + cata6 + cata7) as status1,
(catb1 + catb2 + catb3 + catb4 + catb5 + catb6 + catb7) as status2,
(catc1 + catc2 + catc3 + catc4 + catc5 + catc6 + catc7) as status3,
case when cata1 + cata2 + cata3 + cata4 + cata5 + cata6 + cata7 >= 4 then 'Between_1-5%_IP_Packet_Loss' END||''||
case when catb1 + catb2 + catb3 + catb4 + catb5 + catb6 + catb7 >= 4 then 'Between_5-10%_IP_Packet_Loss' END||''||
case when catc1 + catc2 + catc3 + catc4 + catc5 + catc6 + catc7 >= 4 then '>=10%_IP_Packet_Loss' END as REMARKS
from (
select RNC,NODEB_NAME,NODEBID, date1, date2, date3, date4, date5, date6, date7, round(sum(date1 + date2 + date3 + date4 + date5 + date6 + date7)/7,3) as avg,
case when date1 > 1 and date1 < 5 then '1' END as cata1,
case when date2 > 1 and date2 < 5 then '1' END as cata2,
case when date3 > 1 and date3 < 5 then '1' END as cata3,
case when date4 > 1 and date4 < 5 then '1' END as cata4,
case when date5 > 1 and date5 < 5 then '1' END as cata5,
case when date6 > 1 and date6 < 5 then '1' END as cata6,
case when date7 > 1 and date7 < 5 then '1' END as cata7,
case when date1 >= 5 and date1 < 10 then '1' END as catb1,
case when date2 >= 5 and date2 < 10 then '1' END as catb2,
case when date3 >= 5 and date3 < 10 then '1' END as catb3,
case when date4 >= 5 and date4 < 10 then '1' END as catb4,
case when date5 >= 5 and date5 < 10 then '1' END as catb5,
case when date6 >= 5 and date6 < 10 then '1' END as catb6,
case when date7 >= 5 and date7 < 10 then '1' END as catb7,
case when date1 >= 10 then '1' END as catc1,
case when date2 >= 10 then '1' END as catc2,
case when date3 >= 10 then '1' END as catc3,
case when date4 >= 10 then '1' END as catc4,
case when date5 >= 10 then '1' END as catc5,
case when date6 >= 10 then '1' END as catc6,
case when date7 >= 10 then '1' END as catc7
from (select RNC,NODEB_NAME,NODEBID,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-6 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date1,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-5 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date2,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-4 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date3,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-3 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date4,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-2 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date5,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-1 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date6,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-0 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date7
from PS_ACCESS_NODEB
group by RNC,NODEB_NAME,NODEBID)
group by RNC,NODEB_NAME,NODEBID,date1, date2, date3, date4, date5, date6, date7))
where remarks is not null
group by remarks, substr(RNC,1,3), RNC
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
set feedback off

select 'RNC,NODEB NAME,NODEBID,'||(select trunc(max(timestamp))-6 from PS_ACCESS_NODEB)||','||
(select trunc(max(timestamp))-5 from PS_ACCESS_NODEB)||','||
(select trunc(max(timestamp))-4 from PS_ACCESS_NODEB)||','||
(select trunc(max(timestamp))-3 from PS_ACCESS_NODEB)||','||
(select trunc(max(timestamp))-2 from PS_ACCESS_NODEB)||','||
(select trunc(max(timestamp))-1 from PS_ACCESS_NODEB)||','||
(select trunc(max(timestamp))-0 from PS_ACCESS_NODEB)||',AVERAGE,Consistency >1% but <5%,Consistency >=5% but <10%,Consistency >=10%,REMARKS' from dual;

select RNC||','||NODEB_NAME||','||NODEBID||','||date1||','||date2||','||date3||','||date4||','||date5||','||date6||','||date7||','||avg||','||
(cata1 + cata2 + cata3 + cata4 + cata5 + cata6 + cata7)||','||
(catb1 + catb2 + catb3 + catb4 + catb5 + catb6 + catb7)||','||
(catc1 + catc2 + catc3 + catc4 + catc5 + catc6 + catc7)||','||
case when cata1 + cata2 + cata3 + cata4 + cata5 + cata6 + cata7 >= 4 then 'Consistent >1% but <5%' END||''||
case when catb1 + catb2 + catb3 + catb4 + catb5 + catb6 + catb7 >= 4 then 'Consistent >=5% but <10%' END||''||
case when catc1 + catc2 + catc3 + catc4 + catc5 + catc6 + catc7 >= 4 then 'Consistent >=10%' END
from (
select RNC,NODEB_NAME,NODEBID, date1, date2, date3, date4, date5, date6, date7, round(sum(date1 + date2 + date3 + date4 + date5 + date6 + date7)/7,3) as avg,
case when date1 > 1 and date1 < 5 then '1' END as cata1,
case when date2 > 1 and date2 < 5 then '1' END as cata2,
case when date3 > 1 and date3 < 5 then '1' END as cata3,
case when date4 > 1 and date4 < 5 then '1' END as cata4,
case when date5 > 1 and date5 < 5 then '1' END as cata5,
case when date6 > 1 and date6 < 5 then '1' END as cata6,
case when date7 > 1 and date7 < 5 then '1' END as cata7,
case when date1 >= 5 and date1 < 10 then '1' END as catb1,
case when date2 >= 5 and date2 < 10 then '1' END as catb2,
case when date3 >= 5 and date3 < 10 then '1' END as catb3,
case when date4 >= 5 and date4 < 10 then '1' END as catb4,
case when date5 >= 5 and date5 < 10 then '1' END as catb5,
case when date6 >= 5 and date6 < 10 then '1' END as catb6,
case when date7 >= 5 and date7 < 10 then '1' END as catb7,
case when date1 >= 10 then '1' END as catc1,
case when date2 >= 10 then '1' END as catc2,
case when date3 >= 10 then '1' END as catc3,
case when date4 >= 10 then '1' END as catc4,
case when date5 >= 10 then '1' END as catc5,
case when date6 >= 10 then '1' END as catc6,
case when date7 >= 10 then '1' END as catc7
from (select RNC,NODEB_NAME,NODEBID,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-6 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date1,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-5 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date2,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-4 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date3,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-3 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date4,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-2 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date5,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-1 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date6,
max(decode(trunc(TIMESTAMP,'DD'),(select trunc(max(timestamp))-0 from PS_ACCESS_NODEB),DROP_MEANS,NULL)) as date7
from PS_ACCESS_NODEB
group by RNC,NODEB_NAME,NODEBID)
group by RNC,NODEB_NAME,NODEBID,date1, date2, date3, date4, date5, date6, date7);

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

BODY=${BASE_DIR}/config/packet_loss_body.txt
EMAIL_LIST=${BASE_DIR}/config/packet_loss_list.txt
SENDER=elixir@globe.com.ph
HEADER=${BASE_DIR}/config/packet_loss_header.cfg
TRAILER=${BASE_DIR}/config/packet_loss_trailer.cfg
REPORT=${BASE_DIR}/REPORT_FILES/PS
deyt=`date "+%Y%m%d"`
weekno=`weekno | grep WEEKNO | cut -f 2 -d :`
SUBJECT=`echo "Packet Loss Report for week ${weekno}"`
ATTACH=${REPORT}/packet_loss_report_${weekno}.csv.gz

echo "`date` Processing..."
extract | grep , > ${REPORT}/packet_loss_report_${weekno}.csv

#COMPOSE EMAIL BODY
cat $HEADER > ${BODY}
summary > /apps/DASHBOARD/REPORTS/.packet_loss_summary
for i in `cat /apps/DASHBOARD/REPORTS/.packet_loss_summary | cut -f 1 -d , | sort -u`
do
echo $i | sed -e 's/_/ /g' >>${BODY}
echo >> ${BODY}
echo "&emsp;&emsp;REGION&emsp;&emsp;&emsp;&emsp;&emsp;RNC&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;TOTAL" >> ${BODY}
for j in `cat /apps/DASHBOARD/REPORTS/.packet_loss_summary | grep $i | cut -f 2,3,4 -d ,`
do
xregion=`echo $j | cut -f 1 -d ,`
xrnc=`echo $j | cut -f 2 -d ,`
xtotal=`echo $j | cut -f 3 -d ,`
echo "&emsp;&emsp;&emsp;${xregion}&emsp;&emsp;&emsp;${xrnc}&emsp;&emsp;&emsp;${xtotal}" >> ${BODY}
done
echo >> ${BODY}
echo >> ${BODY}
done
cat $TRAILER >> ${BODY}

gzip -f ${REPORT}/packet_loss_report_${weekno}.csv

#EMAIL DATA

#DEBUG
#echo "/apps/EMAIL/bin/email.ksh ${SENDER} ${EMAIL_LIST} ${BODY} "${SUBJECT}" ${ATTACH}"


/apps/EMAIL/bin/email.ksh ${SENDER} ${EMAIL_LIST} ${BODY} "${SUBJECT}" ${ATTACH}

echo "`date` Done..."

#END PROGRAM
