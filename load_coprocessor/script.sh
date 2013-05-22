#! /bin/bash

#Author: Iain Wright (iain.wright@telescope.tv)
#Author: John Weatherford (john.weatherford@telescope.tv)

# must pass in a Jar
if test -z $1
then
  echo "ERROR: Pass a jar as the paramter to this script"
  echo "IE: ./script.sh HbaseCoprocessors.jar"
  exit 1
fi

# must be run as root
if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" 1>&2
   exit 1
fi

# increment jar
EPOCH_NOW=`date +%s`
mkdir ARCHIVED/${EPOCH_NOW}
INPUT_JAR=`echo $1 | cut -f1 -d.`
OUTPUT_JAR=${INPUT_JAR}${EPOCH_NOW}.jar
mv $1 ${OUTPUT_JAR}

# get value of last jar loaded for hdfs cleanup
OLD_JAR=`cat last_load.txt`

# load into hdfs
echo "Loading ${OUTPUT_JAR} into HDFS"
if [ "${OLD_JAR}" == "" ]; then
  echo "last_load.txt is empty, I dont know what to clean up from the last run"
else
  hadoop fs -rm /${OLD_JAR}
  hadoop fs -expunge
fi
hadoop fs -put ${OUTPUT_JAR} /${OUTPUT_JAR}
su -m hdfs -c 'hadoop fs -chmod 777 /${OUTPUT_JAR}'
su -m hdfs -c 'hadoop fs -chown hbase:hbase /${OUTPUT_JAR}'
hadoop fs -ls /${OUTPUT_JAR}
echo "$OUTPUT_JAR loaded into HDFS"

# load into hbase
echo "Loading ${OUTPUT_JAR} into HBASE"
shift
cat > hbase_script <<- _EOF1_
disable 'test_table'
disable 'test_table2'

alter 'test_table', METHOD => 'table_att_unset', NAME => 'coprocessor\$1'
alter 'test_table', METHOD => 'table_att', 'coprocessor'=>'hdfs:///${OUTPUT_JAR}|telescope.hbase.coprocessors.test|1001|'

alter 'test_table2', METHOD => 'table_att_unset', NAME => 'coprocessor\$1'
alter 'test_table2', METHOD => 'table_att_unset', NAME => 'coprocessor\$2'
alter 'test_table2', METHOD => 'table_att', 'coprocessor'=>'hdfs:///${OUTPUT_JAR}|telescope.hbase.coprocessors.observers.Exporter1|1001|source_family=c'
alter 'test_table2', METHOD => 'table_att', 'coprocessor'=>'hdfs:///${OUTPUT_JAR}|telescope.hbase.coprocessors.observers.Expander2|1002|families=c'

enable 'test_table'
enable 'test_table2'
exit
_EOF1_
hbase shell hbase_script
echo "${OUTPUT_JAR} Loaded into HBASE"
echo "Cleaning up"

# cleanup
mv hbase_script ARCHIVED/$EPOCH_NOW/hbase_script
mv ${OUTPUT_JAR} ARCHIVED/$EPOCH_NOW/${OUTPUT_JAR}
echo $OUTPUT_JAR > last_load.txt
