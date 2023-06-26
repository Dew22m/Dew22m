import os
import time
import operator
from operator import add
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

       
    def block_good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            int(fields[12])
            return True
        except:
            return False
        
     # accessing the shared bucket
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
    
    blockData = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv") 
   
    cleanBlock = blockData.filter(block_good_line)
    
    
    
    # return the miner and the size
    blockAtt = cleanBlock.map(lambda a: (a.split(",")[9],int(a.split(",")[12]))) 
    #aggregate the value per address via reduceByKey
    blockAtt = blockAtt.reduceByKey(operator.add) 
    #sorting according to the size of block
    blockAtt = blockAtt.sortBy(lambda s : s[1], ascending=False) 
    
    
    
    #return the top 10 active miners
    top10miner = blockAtt.takeOrdered(10, key=lambda a: -a[1]) 
     
    # current date and time
    currDateTime  = datetime.now() 
    dateTime = currDateTime .strftime("%d-%m-%Y_%H:%M:%S")

    bucketResource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    finalResult = bucketResource.Object(s3_bucket,'ethereum' + dateTime + '/top10miners.txt')
    finalResult.put(Body=json.dumps(top10miner))
   
    spark.stop()