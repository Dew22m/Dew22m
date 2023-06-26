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

    def block_good_line(line):
        try:
            fields = line.split(',')
            if len(fields) != 19:
                return False
            return True
        except:
            return False

    # accessing the shared bucket
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + \
        ':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hdpConf = spark.sparkContext._jsc.hadoopConfiguration()
    hdpConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hdpConf.set("fs.s3a.access.key", s3_access_key_id)
    hdpConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hdpConf.set("fs.s3a.path.style.access", "true")
    hdpConf.set("fs.s3a.connection.ssl.enabled", "false")

    BlockData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    cleanBlock = BlockData.filter(block_good_line)

    # mapped the lenght of sha3_uncles per the key
    ShData = cleanBlock.map(lambda z:  (
        "sha3_uncles", (len(z.split(",")[4])-2, 1)))
    # aggregate the total length and total rows
    ShData = ShData.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

    # mapped the lenght of logs_bloom per the key
    LogData = cleanBlock.map(lambda z:  (
        "logs_bloom", (len(z.split(",")[5])-2, 1)))
    # aggregate the total length and total rows
    LogData = LogData.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

    # mapped the lenght of transaction_root per the key
    TranRootData = cleanBlock.map(lambda z:  (
        "transaction_root", (len(z.split(",")[6])-2, 1)))
    # aggregate the total length and total rows
    TranRootData = TranRootData.reduceByKey(
        lambda a, b: (a[0]+b[0], a[1]+b[1]))

    # mapped the lenght of state_root per the key
    StatRootData = cleanBlock.map(lambda z:  (
        "state_root", (len(z.split(",")[7])-2, 1)))
    # aggregate the total length and total rows
    StatRootData = StatRootData.reduceByKey(
        lambda a, b: (a[0]+b[0], a[1]+b[1]))

    # mapped the lenght of receipts_root per the key
    RcpRootData = cleanBlock.map(lambda z:  (
        "receipts_root", (len(z.split(",")[8])-2, 1)))
    # aggregate the total length and total rows
    RcpRootData = RcpRootData.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

    # current date and time
    currDateTime = datetime.now()
    dateTime = currDateTime.strftime("%d-%m-%Y_%H:%M:%S")

    bucketResource = boto3.resource('s3',
                                    endpoint_url='http://' + s3_endpoint_url,
                                    aws_access_key_id=s3_access_key_id,
                                    aws_secret_access_key=s3_secret_access_key)

    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/sha3uncles.txt')
    finalResult.put(Body=json.dumps(ShData.take(100)))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/logsbloom.txt')
    finalResult.put(Body=json.dumps(LogData.take(100)))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/transroot.txt')
    finalResult.put(Body=json.dumps(TranRootData.take(100)))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/stateroot.txt')
    finalResult.put(Body=json.dumps(StatRootData.take(100)))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/rcproot.txt')
    finalResult.put(Body=json.dumps(RcpRootData.take(100)))

    spark.stop()
