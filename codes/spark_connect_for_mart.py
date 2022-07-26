import pyspark
from pyspark.sql import SparkSession
import pandas as pd

MAX_MEMORY="10g"
"""
JDBC 드라이버를 이용하여 MySQL DB에 접속하여 Spark SQL을 이용하여 
모델링 팀에서 요청한 Mart 데이터를 준비하였습니다.
"""
spark = SparkSession\
    .builder\
    .appName("MySQL Connection")\
    .config("spark.driver.extraClassPath", "/Users/hoseong/Documents/driver/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar")\
    .config("spark.executor.memory", MAX_MEMORY)\
    .config("spark.driver.memory", MAX_MEMORY)\
    .getOrCreate()

HOST = 'HOST'
DB_USER = 'USER'
DB_PASSWD = 'PWD'
PORT = "3306"
DB_NAME = 'gentleman'

meta_info_df = spark.read\
    .format("jdbc")\
    .option("url", f"jdbc:mysql://{HOST}/{DB_NAME}")\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("dbtable", "metacritic_info").option("user", f"{DB_USER}")\
    .option("password", f"{DB_PASSWD}").load()

meta_user_df = spark.read\
    .format("jdbc")\
    .option("url", f"jdbc:mysql://{HOST}/{DB_NAME}")\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("dbtable", "metacritic_user").option("user", f"{DB_USER}")\
    .option("password", f"{DB_PASSWD}").load()

"""

"""
query = """
SELECT meta_info.appid, meta_user.title, meta_user.username, meta_user.userscore
FROM meta_info
	INNER JOIN meta_user
	ON meta_user.title = meta_info.title
WHERE meta_info.appid IS NOT NULL;
"""
meta_df = spark.sql(query)

meta_df.write.csv("joined_meta.csv")

spark.stop()