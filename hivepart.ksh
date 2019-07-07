set -x
#!/bin/bash
#Script to create hive partition load
#bash hivepart.ksh /home/hduser/hivepart retail.txnrecsbycatdtreg

echo "$0 is starting"
echo "$1 is the source path of txn data sent by the source system"
echo "$2 is the hive table with schema prefixed will be loaded with the above files generically"

rm -f $1/partload.hql

if [ $# -ne 2 ]
then
echo "$0 requires source data path and the target table name to load"
exit 2
fi

echo "$1 is the path"
echo "$2 is the tablename"

for filepathnm in $1/txns_*; do

echo "file with path name is $filepathnm"

filenm=$(basename $filepathnm)
echo "$filenm"

dt=`echo $filenm |awk -F'_' '{print $2}'`
dtfmt=`date -d $dt +'%Y-%m-%d'`
echo $dtfmt

reg=`echo $filenm |awk -F'_' '{print $3}'`
echo $reg

echo "LOAD DATA LOCAL INPATH '$1/$filenm' OVERWRITE INTO TABLE $2 PARTITION (datadt='$dtfmt',region='$reg');" >> $1/partload.hql
done


#cp /home/hduser/hivepart/partload.hql /home/hduser/partload.hql

echo "loading hive table"
hive -f $1/partload.hql










