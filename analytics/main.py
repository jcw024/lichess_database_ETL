from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

if __name__ == '__main__':
    conf = SparkConf()


    spark = SparkSession.builder \
        .master("local") \
        .appName("lichess_games_analysis") \
        .config("spark.jars", "./postgresql-42.2.23.jar") \
        .getOrCreate()
        
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/lichess_games_db") \
        .option("dbtable", "games") \
        .option("user", "database_admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df2 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/lichess_games_db") \
        .option("dbtable", "user_ids") \
        .option("user", "database_admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()


    df.createOrReplaceTempView("games")
    df2.createOrReplaceTempView("user_ids")

    game_count = spark.sql("""SELECT count(analyzed) from games;""")
    game_count.explain()
    game_count.show()
