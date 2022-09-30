#spark-submit AwsRDSRedshiftReadHiveS3Write.py
#Prerequisites for this usecase
#1. RDS service enabled -> connect to the cloud db using a client tool and store some data
#2. Redshift cluster creation -> connect to the cloud dwh using some client tool and store some data
#3. Dev FEDERATED (Connecting with multiple src/tgt) & UNIFIED (writing SQL/DSL/HQL) in one spark Application
# with all dependent libraries added to connect with RDS, RS & S3 ???
#cp /home/hduser/install/aws/all_additional_jars/* /usr/local/spark/jars/
#4. Read from RDS, RS -> wrangle/join/transformation/schema migration -> write to S3 of Cloud and Hive of On-Prem
#spark-submit --jars s3a://com.iz.datalake/rds_s3_redshift_spark_location_jars/* s3a://com.iz.datalake/code/AwsRDSRedshiftReadHiveS3WriteEMR.py
from pyspark.sql.functions import *
from pyspark.sql.types import *

import configparser
def getRdbmsPartData(propfile, sparksess, db, tbl, partcol, lowerbound, upperbound, numpart):
   driver = 'org.postgresql.Driver'
   host = 'jdbc:postgresql://inceptezdbwd28.cihp1jbu2yhe.us-east-1.rds.amazonaws.com'
   port = '5432'
   user = 'inceptezuser'
   passwd = 'inceptezuser'
   url = host + ":" + port + "/" + db
   print(url)
   db_df = sparksess.read.format("jdbc").option("url",url) \
      .option("dbtable", tbl) \
      .option("user", user).option("password", passwd) \
      .option("driver", driver) \
      .option("lowerBound", lowerbound) \
      .option("upperBound", upperbound) \
      .option("numPartitions", numpart) \
      .option("partitionColumn", partcol) \
      .load()
   return db_df

def main():
   from pyspark.sql import SparkSession
   spark = SparkSession.builder\
      .appName("AWS RDS Redshift Read Write to S3/Hive EMR") \
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "s3a://com.iz.datalake/eventLog/") \
      .config("spark.history.fs.logDirectory", "s3a://com.iz.datalake/eventLog/") \
      .enableHiveSupport()\
      .getOrCreate()

   # Set the logger level to error
   spark.sparkContext.setLogLevel("ERROR")
   sc=spark.sparkContext
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "AKIA3YF5CC3LQDFLDUMU")
   sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "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")
   #sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
   #sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
   #sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

   #https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-redshift.html
   #copy all the jars kept in gdrive all_additional_jars to /usr/local/spark/jars location before running this program
   print("Read Patients data from Redshift cluster")
   dfpatients = spark.read.format("io.github.spark_redshift_community.spark.redshift")\
      .option("url","jdbc:redshift://redshift-cluster-wd28.cn3haetn4qhf.us-east-1.redshift.amazonaws.com:5439/dev?user=inceptezuser&password=Inceptezuser123")\
      .option("forward_spark_s3_credentials", True)\
      .option("dbtable", "public.patients").option("tempdir", "s3a://com.inceptez.shellbucket/redshifttempdata/").load()
   #Redshift (NV bulk load/dump)-> s3 (NV multipart/v2/v4 accellerations)-> spark df will be created
   # Redshift -> spark df will be created by using sequential read of data
   print("Data from RedShift")
   dfpatients.cache()
   dfpatients.show(5,False)
   dfpatients.createOrReplaceTempView("redshiftpatients")

   print("Read Drugs data from RDS DB")
   drugs_query = """(select * from healthcare.drugs ) query """
   drugs_df=getRdbmsPartData('/tmp/connection_rds.prop',spark,'dev',drugs_query,"uniqueid",1,100,4)
   #healthcare.drugs
   drugs_df.show()
   drugs_df.createOrReplaceTempView("RDSdrugs")

   print("Wrangle RDS and Redshift drugs and patients data")
   drugsPatientsDF = spark.sql("""select d.*,p.* from RDSdrugs d inner join redshiftpatients p on d.uniqueid=p.drugid""")
   drugsPatientsDF.show(5, False)

   print("Load the wrangled into on-prem Hive")
   drugsPatientsDF.write.mode("overwrite").partitionBy("loaddt").saveAsTable("default.patient_drugs_part")
   print("Show the hive table data")
   spark.read.table("default.patient_drugs_part").show(3)

   print("Load the wrangled into Cloud S3")
   drugsPatientsDF.select("uniqueid","drugname","condition","rating","date","gender","dependents","multipleillness","paymentmethod","drugcharges","loaddt")\
      .coalesce(1).write.mode("overwrite").partitionBy("loaddt").csv("s3a://com.iz.datalake/patients_drugswd28/")
#uniqueid int, drugname string, condition string,rating string,date date,gender string,dependents string,multipleillness string,paymentmethod string,drugcharges float,loaddt date
   print("Spark AWS RDS Redshift Read Write to S3/Hive App Completed Successfully")
main()
