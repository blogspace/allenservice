#!/bin/bash

hive -e "\
add jar \"hdfs://datanode:9000/jars/LowerUDF-jar-with-dependencies.jar\";\
create temporary function LowerUDF as 'demotest.LowerUDF';
select LowerUDF(\"bigdata\") from log.logclean limit 3;
"