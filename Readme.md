# 1 Preparation on the HDP cluster 

**Note:** The description and code of this repository is for HDP 2.3.2.x and 2.4.0.0

For Hortonworks Data Platform 2.4 the Spark assembly file is not on HDFS. It is helpful to (or ask the HDP admin to) copy the assembly to its default location `hdfs://hdp//hdp/apps/2.4.0.0-169/spark//spark/`

- HDP 2.3.2: 
	- Version: 2.3.2.0-2950
	- Spark Jar: /usr/hdp/2.3.2.0-2950/spark/lib/spark-assembly-1.4.1.2.3.2.0-2950-hadoop2.7.1.2.3.2.0-2950.jar
- HDP 2.4.0: 
	- Version: 2.4.0.0-169  
	- Spark Jar: /usr/hdp/2.4.0.0-169/spark/lib/spark-assembly-1.6.0.2.4.0.0-169-hadoop2.7.1.2.4.0.0-169.jar

```bash
sudo su - hdfs
HDP_VERSION=2.4.0.0-169
SPARK_JAR=spark-assembly-1.6.0.2.4.0.0-169-hadoop2.7.1.2.4.0.0-169.jar

hdfs dfs -mkdir "/hdp/apps/$HDP_VERSION/spark/"
hdfs dfs -put "/usr/hdp/$HDP_VERSION/spark/lib/$SPARK_JAR" "/hdp/apps/$HDP_VERSION/spark/spark-hdp-assembly.jar"
```


# 2 Load data and copy it into HDFS

Only a small data set, however sufficient for a sample

```bash
wget https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
```

Create a project folder in HDFS using WebHDFS

```bash
export WEBHDFS_HOST=http://beebox01:50070

curl -X PUT "$WEBHDFS_HOST/webhdfs/v1/tmp/simple-project?op=MKDIRS"
# {"boolean":true}
```

Upload data to project folder using WebHDFS

```bash
curl -i -X PUT "$WEBHDFS_HOST/webhdfs/v1/tmp/simple-project/iris.data?op=CREATE&overwrite=true"
# HTTP/1.1 307 TEMPORARY_REDIRECT
# Cache-Control: no-cache
# Expires: Sun, 10 Apr 2016 11:35:44 GMT
# Date: Sun, 10 Apr 2016 11:35:44 GMT
# Pragma: no-cache
# Expires: Sun, 10 Apr 2016 11:35:44 GMT
# Date: Sun, 10 Apr 2016 11:35:44 GMT
# Pragma: no-cache
# Location: http://beebox06.localdomain:50075/webhdfs/v1/tmp/simple-project/simple-project_2.10-1.0.jar?op=CREATE&namenoderpcaddress=beebox01.localdomain:8020&createflag=&# createparent=true&overwrite=true
# Content-Type: application/octet-stream
# Content-Length: 0
# Server: Jetty(6.1.26.hwx)

LOCATION="http://beebox06.localdomain:50075/webhdfs/v1/tmp/simple-project/simple-project_2.10-1.0.jar?op=CREATE&namenoderpcaddress=beebox01.localdomain:8020&createflag=&createparent=true&overwrite=true"

curl -i -X PUT -T "iris.data" "$LOCATION"
```



# 3 Submit a project to Spark from your workstation via python script

Note: Install `requests` module: `pip install requests`


## 3.1 Create Spark project and copy it to HDFS

Simple project to calculate mean of each feature per species

```bash
cd simple-project
```

Copy the right `iris.sbt-HDP*` to `iris.sbt`

```bash
sbt package

export APP_FILE=simple-project_2.10-1.0.jar

curl -i -X PUT "$WEBHDFS_HOST/webhdfs/v1/tmp/simple-project/$APP_FILE?op=CREATE&overwrite=true"
# take Location header, see above

LOCATION="http://..."

curl -i -X PUT -T "target/scala-2.10/$APP_FILE" "$LOCATION"

cd ..
```

## 3.2 Submit job

If you use Knox, set the KNOX_CREDENTIALS environment variable

```bash
export KNOX_CREDENTIALS=username:password
```

Then edit `project.cfg` and call

```bash
python spark-remote-submit.py
# Checking project folder ...
# Uploading App Jar ...
# Uploading Spark properties
# Creating Spark Job file ...
# Submitting Spark Job ...
# 
# ==> Job tracking URL: http://192.168.56.239:8088/ws/v1/cluster/apps/application_1460392460492_0025
```

## 3.3 Track job

```bash
curl -s http://192.168.56.239:8088/ws/v1/cluster/apps/application_1460392460492_0025 | jq .
# {
#   "app": {
#     "id": "application_1460392460492_0025",
#     "user": "dr.who",
#     "name": "SimpleProject",
#     "queue": "default",
#     "state": "FINISHED",
#     "finalStatus": "SUCCEEDED",
#     "progress": 100,
#     "trackingUI": "History",
#     "trackingUrl": "http://hdp-sa-239.localdomain:8088/proxy/application_1460392460492_0025/",
#     "diagnostics": "",
#     "clusterId": 1460392460492,
#     "applicationType": "YARN",
#     "applicationTags": "",
#     "startedTime": 1460413592029,
#     "finishedTime": 1460413612191,
#     "elapsedTime": 20162,
#     "amContainerLogs": "http://hdp-sa-239:8042/node/containerlogs/container_e03_1460392460492_0025_01_000001/dr.who",
#     "amHostHttpAddress": "hdp-sa-239:8042",
#     "allocatedMB": -1,
#     "allocatedVCores": -1,
#     "runningContainers": -1,
#     "memorySeconds": 85603,
#     "vcoreSeconds": 51,
#     "preemptedResourceMB": 0,
#     "preemptedResourceVCores": 0,
#     "numNonAMContainerPreempted": 0,
#     "numAMContainerPreempted": 0,
#     "logAggregationStatus": "SUCCEEDED"
#   }
# }
```



# 4 Manually submit a project to Spark from your workstation


## 4.1 Create Spark project and copy it to HDFS

see 3.1


## 4.2 Populate the control files for the YARN REST API

### 4.2.1 Spark properties

Copy `spark-yarn.properties.template` to `spark-yarn.properties` and edit keys if necessary.

Upload `spark-yarn.properties` to the project folder in HDFS

```bash
curl -i -X PUT "$WEBHDFS_HOST/webhdfs/v1/tmp/simple-project/spark-yarn.properties?op=CREATE&overwrite=true"
# take Location header, see above

LOCATION="http://..."
curl -i -X PUT -T "spark-yarn.properties" "$LOCATION"
```

### 4.2.2 The JSON job file for the YARN REST API

For caching purposes Spark needs file sizes and modification times of all project files. The following commands use the json processor `jq` from [https://stedolan.github.io/jq/](https://stedolan.github.io/jq/)

```bash
curl -s "$WEBHDFS_HOST/webhdfs/v1/hdp/apps/2.4.0.0-169/spark/spark-hdp-assembly.jar?op=GETFILESTATUS" \
| jq '.FileStatus | {size: .length, timestamp: .modificationTime}'
# {
#   "size": 191724610,
#   "timestamp": 1460219553714
# }
curl -s "$WEBHDFS_HOST/webhdfs/v1/tmp/simple-project/simple-project_2.10-1.0.jar?op=GETFILESTATUS" \
| jq '.FileStatus | {size: .length, timestamp: .modificationTime}'
# {
#   "size": 10270,
#   "timestamp": 1460288240001
# }
curl -s "$WEBHDFS_HOST/webhdfs/v1/tmp/simple-project/spark-yarn.properties?op=GETFILESTATUS" \
| jq '.FileStatus | {size: .length, timestamp: .modificationTime}'
# {
#   "size": 767,
#   "timestamp": 1460289956356
# }
```

Copy `spark-yarn.json.template` to `spark-yarn.json` and edit all `local-resources`:

- `resource`: adapt namenode address
- `timestamp`, `size`: according to the above values

Next edit the `enviroments` section and modify the keys `SPARK_YARN_CACHE_FILES`, `SPARK_YARN_CACHE_FILES_FILE_SIZES`, `SPARK_YARN_CACHE_FILES_TIME_STAMPS` so that file names, timestamps and sizes are the same as in the `local_resources` section.

Note: The properties file is only for the Application Master and can be ignored here.

Also adapt versions in `CLASSPATH` of section `environment` and in the `command`

## 4.3 Submit a Spark job to YARN

### 4.3.1 Create a YARN application

```bash
export HADOOP_RM=http://beebox04:8088

curl -s -X POST $HADOOP_RM/ws/v1/cluster/apps/new-application | jq .
# {
#   "application-id": "application_1460195242962_0051",
#   "maximum-resource-capability": {
#     "memory": 4000,
#     "vCores": 3
#   }
# }
```

Edit `spark-yarn.json` again and modify the `application-id` to hold the newly create id.


### 4.3.2 Submit the Spark job

```bash 
curl -s -i -X POST -H "Content-Type: application/json" $HADOOP_RM/ws/v1/cluster/apps --data-binary spark-yar.json 
# HTTP/1.1 100 Continue
# 
# HTTP/1.1 202 Accepted
# Cache-Control: no-cache
# Expires: Sun, 10 Apr 2016 13:02:47 GMT
# Date: Sun, 10 Apr 2016 13:02:47 GMT
# Pragma: no-cache
# Expires: Sun, 10 Apr 2016 13:02:47 GMT
# Date: Sun, 10 Apr 2016 13:02:47 GMT
# Pragma: no-cache
# Content-Type: application/json
# Location: http://beebox04:8088/ws/v1/cluster/apps/application_1460195242962_0054
# Content-Length: 0
# Server: Jetty(6.1.26.hwx)
```


### 4.3.3 Get job status and result

Take the `Location` header from above:

```bash
curl -s http://beebox04:8088/ws/v1/cluster/apps/application_1460195242962_0054 | jq .
# {
#   "app": {
#     "id": "application_1460195242962_0054",
#     "user": "dr.who",
#     "name": "IrisApp",
#     "queue": "default",
#     "state": "FINISHED",
#     "finalStatus": "SUCCEEDED",
#     "progress": 100,
#     "trackingUI": "History",
#     "trackingUrl": "http://beebox04.localdomain:8088/proxy/application_1460195242962_0054/",
#     "diagnostics": "",
#     "clusterId": 1460195242962,
#     "applicationType": "YARN",
#     "applicationTags": "",
#     "startedTime": 1460293367576,
#     "finishedTime": 1460293413568,
#     "elapsedTime": 45992,
#     "amContainerLogs": "http://beebox03.localdomain:8042/node/containerlogs/container_e29_1460195242962_0054_01_000001/dr.who",
#     "amHostHttpAddress": "beebox03.localdomain:8042",
#     "allocatedMB": -1,
#     "allocatedVCores": -1,
#     "runningContainers": -1,
#     "memorySeconds": 172346,
#     "vcoreSeconds": 112,
#     "queueUsagePercentage": 0,
#     "clusterUsagePercentage": 0,
#     "preemptedResourceMB": 0,
#     "preemptedResourceVCores": 0,
#     "numNonAMContainerPreempted": 0,
#     "numAMContainerPreempted": 0,
#     "logAggregationStatus": "SUCCEEDED"
#   }
# }
```

#5 Get the result

Note: The partition name depends on run, find it via WebHDFS and `LISTSTATUS`

```bash
curl -s -L $WEBHDFS_HOST/webhdfs/v1/tmp/iris/means/part-r-00000-a1d003bf-246b-47b5-9d61-10dede1c3981?op=OPEN | jq .
# {
#   "species": "Iris-setosa",
#   "avg(sepalLength)": 5.005999999999999,
#   "avg(sepalWidth)": 3.4180000000000006,
#   "avg(petalLength)": 1.464,
#   "avg(petalWidth)": 0.2439999999999999
# }
# {
#   "species": "Iris-versicolor",
#   "avg(sepalLength)": 5.936,
#   "avg(sepalWidth)": 2.77,
#   "avg(petalLength)": 4.26,
#   "avg(petalWidth)": 1.3260000000000003
# }
# {
#   "species": "Iris-virginica",
#   "avg(sepalLength)": 6.587999999999998,
#   "avg(sepalWidth)": 2.9739999999999998,
#   "avg(petalLength)": 5.552,
#   "avg(petalWidth)": 2.026
# }
```
