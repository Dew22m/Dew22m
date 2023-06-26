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

    def scam_good_line(line):
        try:
            fields = line.split(',')
            if len(fields) != 8:
                return False
            int(fields[0])
            return True
        except:
            return False

    def tran_good_line(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            int(fields[11])
            int(fields[8])
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

    def fico(a):
        if a[1][1][1] == 'Fake ICO':
            return True

    def phishing(a):
        if a[1][1][1] == 'Phishing':
            return True

    def scam(a):
        if a[1][1][1] == 'Scamming':
            return True

    tranData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scamData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")

    cleanTran = tranData.filter(tran_good_line)
    cleanScam = scamData.filter(scam_good_line)

    # return Scams' address and its vaue
    scamAtt = cleanScam.map(lambda b: (
        b.split(',')[6], (int(b.split(',')[0]), str(b.split(',')[4]))))
    # return transcations' address and the date (and) per value
    tranAtt = cleanTran.map(lambda b: (b.split(',')[6], (time.strftime(
        "%m %y", time.gmtime(int(b.split(',')[11]))), int(b.split(',')[7]))))

    # join the transactions' address and the related date and its value with the relevent id to each category from scams
    joinData = tranAtt.join(scamAtt)

    # Mapped the scams' id to each value
    scamId = joinData.map(lambda a: (a[1][1][0], a[1][0][1]))  # (id, value)
    # aggregating the the value based on id
    lucScam = scamId.reduceByKey(operator.add)
    # sorting and grouping the top 10 popular scams
    top10Scam = lucScam.takeOrdered(10, key=lambda a: -a[1])

    # finding the most popular type of scam considering time
    # mapping the date and reletive aggregated value
    DateScam = joinData.filter(scam).map(lambda a: (
        a[1][0][0], a[1][0][1])).reduceByKey(operator.add).sortByKey(ascending=True)
    DatePhishing = joinData.filter(phishing).map(lambda a: (
        a[1][0][0], a[1][0][1])).reduceByKey(operator.add).sortByKey(ascending=True)
    DateFico = joinData.filter(fico).map(lambda a: (
        a[1][0][0], a[1][0][1])).reduceByKey(operator.add).sortByKey(ascending=True)

    # current date and time
    currDateTime = datetime.now()  # current date and time
    dateTime = currDateTime.strftime("%d-%m-%Y_%H:%M:%S")

    bucketResource = boto3.resource('s3',
                                    endpoint_url='http://' + s3_endpoint_url,
                                    aws_access_key_id=s3_access_key_id,
                                    aws_secret_access_key=s3_secret_access_key)

    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/lucscam.txt')
    finalResult.put(Body=json.dumps(top10Scam))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/datescam.txt')
    finalResult.put(Body=json.dumps(DateScam.take(100)))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/datephishing.txt')
    finalResult.put(Body=json.dumps(DatePhishing.take(100)))
    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/datefico.txt')
    finalResult.put(Body=json.dumps(DateFico.take(100)))

    spark.stop()
