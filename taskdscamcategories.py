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
            if len(fields) != 15:
                return False
            int(fields[8])
            int(fields[11])
            return True
        except:
            return False

    def scam_good_line(line):
        try:
            fields = line.split(',')
            if len(fields) != 8:
                return False
            int(fields[0])
            return True
        except:
            return False

    # access shared bucket
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

    transData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scamData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")

    cleanScam = scamData.filter(scam_good_line)

    cleanTrans = transData.filter(good_line_trans)

   # return transcations' address and the date (and) per value
    transAtt = cleanTrans.map(lambda b: (b.split(',')[6], (time.strftime(
        "%m %y", time.gmtime(int(b.split(',')[11]))), int(b.split(',')[7]))))
    # return Scams' address and per each id and the category
    scamAtt = cleanScam.map(lambda b: (
        b.split(',')[6], (int(b.split(',')[0]), str(b.split(',')[4]))))

    # join the transactions' address and the related date  and its value with the relevent id to each category from scams
    joinData = transAtt.join(scamAtt)

    # Mapped the scams' category to each value
    scamId = joinData.map(lambda a: (a[1][1][1], a[1][0][1]))
    # aggregating the the value based on id
    lucScam = scamId.reduceByKey(operator.add)
    # sorting and grouping the top 10 popular scams
    top10scam = lucScam.takeOrdered(10, key=lambda a: -a[1])

    # current date and time
    currDateTime = datetime.now()  # current date and time
    dateTime = currDateTime.strftime("%d-%m-%Y_%H:%M:%S")

    bucketResource = boto3.resource('s3',
                                    endpoint_url='http://' + s3_endpoint_url,
                                    aws_access_key_id=s3_access_key_id,
                                    aws_secret_access_key=s3_secret_access_key)

    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/lucscamcat.txt')
    finalResult.put(Body=json.dumps(top10scam))

    spark.stop()
