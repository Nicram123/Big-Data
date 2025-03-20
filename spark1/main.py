from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, date_format, from_unixtime, to_date, to_timestamp, from_utc_timestamp, to_utc_timestamp, current_date, current_timestamp, year, month, dayofyear

spark = SparkSession.builder.appName("DateFunctions").getOrCreate()

# Przykładowe dane

## 1
kolumny = ["timestamp", "unix", "Date"]
dane = [
    ("2015-03-22T14:13:34", 1646641525847, "May, 2021"),
    ("2015-03-22T15:03:18", 1646641557555, "Mar, 2021"),
    ("2015-03-22T14:38:39", 1646641578622, "Jan, 2021")
]

df_dates = spark.createDataFrame(dane, kolumny) \
    .withColumn("current_date", current_date()) \
    .withColumn("current_timestamp", current_timestamp())

# Krok 1: Poprawione parsowanie daty i czasu
df_transformed = df_dates \
    .withColumn("unix_timestamp_col", unix_timestamp(df_dates["timestamp"], "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("formatted_date", date_format(df_dates["timestamp"], "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("from_unix_time_col", from_unixtime(df_dates["unix"] / 1000)) \
    .withColumn("to_date_col", to_date(df_dates["timestamp"])) \
    .withColumn("to_timestamp_col", to_timestamp(df_dates["timestamp"])) \

# Krok 2: Użycie `from_unix_time_col` w następnych transformacjach
df_final = df_transformed \
    .withColumn("from_utc_to_local", from_utc_timestamp(df_transformed["from_unix_time_col"], "Europe/Warsaw")) \
    .withColumn("local_to_utc", to_utc_timestamp(df_transformed["from_unix_time_col"], "UTC")) \
    .withColumn("year_col", year(df_transformed["to_date_col"])) \
    .withColumn("month_col", month(df_transformed["to_date_col"])) \
    .withColumn("day_of_year_col", dayofyear(df_transformed["to_date_col"]))

# użycie day , month i dayofyear


df_final.show(truncate=False)






###################
###################

##  2

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
actorsUrl = "C:/Users/marci/OneDrive/Pulpit/Big Data/actors.csv"
# "/FileStore/tables/Files/df_actors_invalid_category.csv"
#
schema = StructType([
    StructField("imdb_title_id", StringType(), True),  # identyfikator filmu
    StructField("ordering", IntegerType(), True),  # numer porządkowy
    StructField("imdb_name_id", StringType(), True),  # identyfikator aktora
    StructField("category", StringType(), True),  # kategoria (np. rola)
    StructField("job", StringType(), True),  # rola aktora
    StructField("characters", StringType(), True)  # postać, którą aktor gra
])
df_actors = spark.read.csv(actorsUrl, header=True, schema=schema)
df_actors.show(truncate=False)   # pełna dlugosc bez skracania wartosci w komorkach



########
########

## 4. Zapis do Parquet Json

output_path_parquet = "C:/Users/marci/OneDrive/Pulpit/Big Data/actors.parquet"
output_path_json = "C:/Users/marci/OneDrive/Pulpit/Big Data/actors.json"

df_actors.write.mode("overwrite").parquet(output_path_parquet)
df_actors.write.mode("overwrite").json(output_path_json)

# odczytaj zapisane pliki
df_parquet = spark.read.parquet(output_path_parquet)
df_json = spark.read.json(output_path_json)


df_parquet.show()
df_json = df_json.select(df_actors.columns)  # Ustawienie kolejności kolumn na taką samą jak w df_actors
df_json.show()
# df_json.show()

###########################
###########################
## 3 zadanie 
actorsPermissive = spark.read.format('csv').option("header", "true").schema(schema).option("mode", "PERMISSIVE").load(actorsUrl)
actorsDropmalformed = spark.read.format('csv').option("header", "true").schema(schema).option("mode", "DROPMALFORMED").load(actorsUrl)
actorsFailfast = spark.read.format('csv').option("header", "true").schema(schema).option("mode", "FAILFAST").load(actorsUrl)
actorsBadRecords = spark.read.format("csv").option("header", "true").schema(schema).option("badRecordsPath", "C:/Users/marci/OneDrive/Pulpit/Big Data")
from pyspark.sql.functions import *
# zepsucie pliku
actorsPermissive = actorsPermissive.withColumn("ordering", when(col("ordering") > 7, 'nan').otherwise(col("ordering")))
actorsPermissive.write.mode("overwrite").csv("C:/Users/marci/OneDrive/Pulpit/Big Data/zepsuty.csv", header=True)
crashedDfPermissive = spark.read.format('csv').option("header", "true").schema(schema).option("mode", "PERMISSIVE").load("C:/Users/marci/OneDrive/Pulpit/Big Data/zepsuty.csv")
crashedDfPermissive.show() # wartości null w kolumnie "ordering"
crashedDfDropmalformed = spark.read.format('csv').option("header", "true").schema(schema).option("mode", "DROPMALFORMED").load("C:/Users/marci/OneDrive/Pulpit/Big Data/zepsuty.csv")
crashedDfDropmalformed.show() # usunął wiersze z null w kolumnie "ordering"
crashedDfFailfast = spark.read.format('csv').option("header", "true").schema(schema).option("mode", "FAILFAST").load("C:/Users/marci/OneDrive/Pulpit/Big Data/zepsuty.csv")
# crashedDfFailfast.show() # przerwał operację, gdy natrafił na błąd
crashedDfBadRecords = spark.read.format("csv").option("header", "true").schema(schema).option("badRecordsPath", "C:/Users/marci/OneDrive/Pulpit/Big Data").load("C:/Users/marci/OneDrive/Pulpit/Big Data/zepsuty.csv")
# display(crashedDfBadRecords) # usunął wiersze z null w kolumnie "ordering"