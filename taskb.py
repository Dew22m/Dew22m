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

    def good_line_trans(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            int(fields[7])
            int(fields[11])
            return True
        except:
            return False

    def good_line_con(line):
        try:
            fields = line.split(',')
            if len(fields) != 6:
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

    transData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    conData = spark.sparkContext.textFile(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    cleanTrans = transData.filter(good_line_trans)
    cleanCon = conData.filter(good_line_con)

    # return address and the value of transactions
    transAtt = cleanTrans.map(lambda z: (
        z.split(",")[6], int(z.split(",")[7])))
    # return address and the bytecode of contracts
    conAtt = cleanCon.map(lambda z: (z.split(",")[0], z.split(",")[1]))

    # join the address and the value and the bytecode in contracts and transaction
    joinData = transAtt.join(conAtt)
    # map the join data to get address and the value
    joinData = joinData.map(lambda z: (z[0], z[1][0]))
    # aggregate the value per each address through reduceByKey
    joinData = joinData.reduceByKey(operator.add)

    # return the top 10 popular services
    top10Con = joinData.takeOrdered(10, key=lambda a: -a[1])

    # current date and time
    currDateTime = datetime.now()
    dateTime = currDateTime.strftime("%d-%m-%Y_%H:%M:%S")

    bucketResource = boto3.resource('s3',
                                    endpoint_url='http://' + s3_endpoint_url,
                                    aws_access_key_id=s3_access_key_id,
                                    aws_secret_access_key=s3_secret_access_key)

    finalResult = bucketResource.Object(
        s3_bucket, 'ethereum' + dateTime + '/top10con.txt')
    finalResult.put(Body=json.dumps(top10Con))

    spark.stop()
