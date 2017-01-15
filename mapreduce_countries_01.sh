#!/bin/bash
#
# script to update hdfs with the latest versions of the related mapreduce files
# to prepare the environment and to finaly run a mapreduce job
#
# the mapreduce job uses a configuration file where the key fields, the field types,
# and the field used for calculations are defined. it processes a CSV file with rows
# of data.
#
# the JaRE Business Rule Engine is then used to filter the data that is used for the 
# mapreduce process.
#
# finally the result is copied to the local filesystem and then a groovy script is run
# that formats the output and merges it with a HTML template (Apache Velocity) to display
# the results on a web page.
#
#
#
# last update: uwe geercken 2017-01-13
#

# check if the required arguments to the script are found
# we require either no arguments (the defaults are used) or three arguments
if (($# != 0 && $# != 3 ))
then
	echo "error: invalid number of arguments"
	echo "usage: $0"
	echo "or:    $0 [hdfs input_dir] [hdfs output_dir] [ruleengine project file]"
	echo
	exit 1
fi

# format date time for output to the console
datetime_format="+%Y-%m-%d %H:%M:%S"
echo $(date "${datetime_format}") "start of process"

if (($# == 0 ))
then
	echo $(date "${datetime_format}") "no arguments specified - using defaults"
fi

# the home folder in hdfs where we start from
hdfs_home="/user/${USER}"

#  the folder where the files used in the process are located
files_dir="/home/${USER}/development"

# the actual mapreduce library and the class to run
mapreduce_library="processor.jar"
mapreduce_class="com.datamelt.hadoop.data.DataProcessor"

# the hdfs input directory where the data resides
input_dir=${1:-"input_dir"}

# the hdfs output directory of the process
output_dir=${2:-"output_dir"}

# JaRE ruleengine library
ruleengine_library="jare0.79.jar"

# the ruleengine project file
# contains the business logic to apply
ruleengine_project_file=${3:-"business_logic.zip"}

# local output file name
outputfile_name=countries.result

# groovy script name
groovy_script=countries.groovy

# add ruleengine library to hadoop classpath for the JVM to find it
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${files_dir}/${ruleengine_library}

# put the ruleengine library to hdfs
echo $(date "${datetime_format}") "putting ruleengine library [${ruleengine_library}] to hdfs folder: ${hdfs_home}"
hdfs dfs -put -f ${files_dir}/${ruleengine_library} ${hdfs_home}

# put the ruleengine project file (with the rules) to hdfs
echo $(date "${datetime_format}") "putting ruleengine project file [${ruleengine_project_file}] to hdfs folder: ${hdfs_home}"
hdfs dfs -put -f ${files_dir}/${ruleengine_project_file} ${hdfs_home}

# remove hdfs output directory if it exists
echo $(date "${datetime_format}") "removing existing folder: ${hdfs_home}/${output_dir}"
hdfs dfs -rm -r -f ${hdfs_home}/${output_dir}

# run mapreduce job on the files of the input directory and using the ruleengine
echo $(date "${datetime_format}") "running mapreduce job..."
echo $(date "${datetime_format}") "processing from: ${input_dir}, output to: ${output_dir}"

hadoop jar ${files_dir}/${mapreduce_library} ${mapreduce_class} "${input_dir}" "${output_dir}" "${ruleengine_project_file}"
echo $(date "${datetime_format}") "completed mapreduce job"

# output the results to the console
#echo $(date "${datetime_format}") "displaying mapreduce result"
#hdfs dfs -cat ${hdfs_home}/${output_dir}/part-r-*

# remove original results file from local filesystem
echo $(date "${datetime_format}") "removing results file ${files_dir}/${outputfile_name}"
rm -f ${files_dir}/${outputfile_name}

# copy the results to a local file
echo $(date "${datetime_format}") "copying mapreduce result to ${files_dir}/countries.result"
hdfs dfs -copyToLocal ${hdfs_home}/${output_dir}/part-r-00000 ${files_dir}/${outputfile_name}

# generate an html file from the data using groovy and velocity and highcharts
echo $(date "${datetime_format}") "running groovy script to create web page"
groovy ${files_dir}/${groovy_script}

# process ends here
echo $(date "${datetime_format}")  "end of process"

