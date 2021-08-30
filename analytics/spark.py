from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import StorageLevel
from time import time

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local") \
        .appName("lichess_games_analysis") \
        .config("spark.jars", "./postgresql-42.2.23.jar") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.memory.offHeap.enabled", True) \
        .config("spark.memory.offHeap.size", "50g") \
        .getOrCreate()
        
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/lichess_games_db") \
        .option("dbtable", "games") \
        .option("user", "database_admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load().repartition(800) \
        .persist(StorageLevel.DISK_ONLY)

    df2 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/lichess_games_db") \
        .option("dbtable", "(SELECT * FROM user_ids WHERE id = 1) as tmp2") \
        .option("user", "database_admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load().repartition(8) \
        .persist(StorageLevel.DISK_ONLY)

    df3 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/lichess_games_db") \
        .option("dbtable", "user_ids_p") \
        .option("user", "database_admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load().repartition(8) \
        .persist(StorageLevel.DISK_ONLY)


    df.createOrReplaceTempView("games")
    df2.createOrReplaceTempView("user_ids")
    df3.createOrReplaceTempView("user_ids_p")

    t0 = time()
    game_count = spark.sql("""SELECT avg(case when analyzed = TRUE then 1 else 0 END) from games;""")
    game_count.explain()
    game_count.show()
    #user_count = spark.sql("""SELECT count(id) from user_ids;""")
    #user_count.explain()
    #user_count.show()

    print(time() - t0)
