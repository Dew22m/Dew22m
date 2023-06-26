import os
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def good_line_trans(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            int(fields[7])
            return True
        except:
            return False

    # access to the bucket contains datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    

    hdpConf = spark.sparkContext._jsc.hadoopConfiguration()
    hdpConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hdpConf.set("fs.s3a.access.key", s3_access_key_id)
    hdpConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hdpConf.set("fs.s3a.path.style.access", "true")
    hdpConf.set("fs.s3a.connection.ssl.enabled", "false")

    transData = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    cleanTrans = transData.filter(good_line_trans)
    
    #mapped the transaction per key (month)
    transCount = cleanTrans.map(lambda a: (time.strftime("%m %Y",time.gmtime(int(a.split(',')[11]))),1))
    # using month (key) to aggregate the total value of transactions
    trans = transCount.reduceByKey(operator.add)
    
    
     
    currDateTime = datetime.now() # current date and time
    dateTime = currDateTime.strftime("%d-%m-%Y_%H:%M:%S")

    bucketResource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    finalResult = bucketResource.Object(s3_bucket,'ethereum' + dateTime + '/monthlytrans.txt')
    finalResult.put(Body=json.dumps(trans.take(50)))
                           
    
    spark.stop()
    