from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, regexp_replace, regexp_extract, when, count, sum, avg, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag, first, last, row_number

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, regexp_replace, regexp_extract, when, count, sum, avg, udf, array_contains, nullif, ifnull
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import math

# Inicjalizacja sesji Spark
spark = SparkSession.builder.appName("Notebook1").getOrCreate()

# Przykładowe dane
data = [
    (1, "Jan", None, "[1,2,3]"),
    (2, "Anna", "Kowalska", "[4,5]"),
    (3, "Piotr", "Nowak", None),
    (4, None, "Zieliński", "[7,8,9]"),
    (5, "Maria", None, "[10]")
]

columns = ["id", "imie", "nazwisko", "wartosci"]
df = spark.createDataFrame(data, columns)

# Rozbijanie tablicy na pojedyncze wartości
# Rozbijanie tablicy na pojedyncze wartości
df = df.withColumn("wartosci", regexp_replace(col("wartosci"), "[\[\]]", ""))  # git
df = df.withColumn("wartosc", explode(expr("split(wartosci, ',')")))  # git
df = df.withColumn("imie", ifnull(col("imie"), lit("Brak imienia"))) # git
df = df.fillna({"nazwisko": "Brak nazwiska"})
df = df.withColumn("wartosc_num", regexp_extract(col("wartosc"), "(\\d+)", 1).cast("int")) # git
df = df.withColumn("czy_zawiera_5", array_contains(expr("split(wartosci, ',')"), "5"))
#df = df.withColumn("imie", nullif(col("imie"), lit("Maria"))) # nie wiem czemu nie działa
# * zachowujemy kolumny
df = df.selectExpr("*", "nullif(imie, 'Maria') as imie_cleaned")  # git
df = df.drop("wartosci")

# Okno dla funkcji okienkowych
window_spec = Window.partitionBy("imie").orderBy("wartosc_num")
# Dodanie funkcji okienkowych
df = df.withColumn("next_value", lead("wartosc_num").over(window_spec).alias("next_value"))
df = df.withColumn("prev_value", lag("wartosc_num").over(window_spec).alias("prev_value"))
df = df.withColumn("first_value", first("wartosc_num").over(window_spec).alias("first_value"))
df = df.withColumn("last_value", last("wartosc_num").over(window_spec).alias("last_value"))
df = df.withColumn("row_num", row_number().over(window_spec).alias("row_num"))

# Funkcje agregujące
agg_df = df.groupBy("imie").agg(
    count("wartosc_num").alias("liczba_wartosci"),
    sum("wartosc_num").alias("suma_wartosci"),
    avg("wartosc_num").alias("srednia_wartosc")
)   # git


# Zadanie 2
def round_to_nearest_five(value):
    if value is None:
        return None
    return int(round(value / 5.0) * 5)
round_udf = udf(round_to_nearest_five, IntegerType())
df = df.withColumn("wartosc_num_zaokraglona", round_udf(col("wartosc_num")))

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def uppercase_pandas_udf(imie_series: pd.Series) -> pd.Series:
    return imie_series.str.upper()
df = df.withColumn("imie_duze_litery", uppercase_pandas_udf(col("imie")))


df.show()
agg_df.show()









