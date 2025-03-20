from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, lit, year, regexp_replace, when, current_date, current_timestamp, datediff, sum
# Inicjalizacja sesji Spark
spark = SparkSession.builder.appName("SparkExercises").getOrCreate()



# Zadanie 1 - Wczytanie pliku actors.csv
actorsPath = "C:/Users/marci/OneDrive/Pulpit/Big Data/names.csv"
actorsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(actorsPath)
actorsDf.explain(True)
actorsDf = actorsDf.withColumn("ExecutionTime", unix_timestamp())
actorsDf = actorsDf.withColumn("feet", col("height") / 30.48)
most_popular_name = actorsDf.groupBy("name").count().orderBy("count", ascending=False).first()
print(f"Najpopularniejsze imię: {most_popular_name['name']}")
actorsDf = actorsDf.withColumn("age", (datediff(current_date(), col("date_of_birth")) / 365).cast("int"))
actorsDf = actorsDf.drop("bio", "death_details")
for col_name in actorsDf.columns:
    new_col_name = col_name.replace("_", "").capitalize()
    actorsDf = actorsDf.withColumnRenamed(col_name, new_col_name)
actorsDf = actorsDf.orderBy("Name", ascending=True)
actorsDf.show()




# Zadanie 2 - Wczytanie pliku Movies.csv
filePath = "C:/Users/marci/OneDrive/Pulpit/Big Data/movies.csv"
moviesDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath)

# Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
moviesDf = moviesDf.withColumn("ExecutionTime", unix_timestamp())

# Dodaj kolumnę z liczbą lat od publikacji

# moviesDf = moviesDf.withColumn("YearsSinceRelease", lit(2025) - col("year"))
moviesDf = moviesDf.withColumn("years_since_published", year(current_date()) - year(moviesDf["date_published"]))

# Usuń znaki walut i skonwertuj budżet na liczbę
moviesDf = moviesDf.withColumn("BudgetNumeric", regexp_replace(col("budget"), "[^0-9]", "").cast("int"))

# Usuń wiersze z wartościami null
moviesDf = moviesDf.dropna()
moviesDf.show()


# Zadanie 3 - Wczytanie Ratings.csv
rfilePath = "C:/Users/marci/OneDrive/Pulpit/Big Data/ratings.csv"
ratingsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(rfilePath)


ratingsDf = ratingsDf.withColumn("ExecutionTime", unix_timestamp(current_timestamp()))

# Kto ocenia wyzej
males_weighted_sum = ratingsDf.select((col("males_allages_avg_vote") * col("males_allages_votes")).alias("males_weighted"))\
    .agg(sum("males_weighted")).collect()[0][0]
males_total_votes = ratingsDf.select(sum("males_allages_votes")).collect()[0][0]
females_weighted_sum = ratingsDf.select((col("females_allages_avg_vote") * col("females_allages_votes")).alias("females_weighted"))\
    .agg(sum("females_weighted")).collect()[0][0]
females_total_votes = ratingsDf.select(sum("females_allages_votes")).collect()[0][0]
# Liczymy średnią ważoną (sumaryczna ocena / liczba głosów)
males_global_avg = males_weighted_sum / males_total_votes if males_total_votes else None
females_global_avg = females_weighted_sum / females_total_votes if females_total_votes else None
# Kto ocenia wyżej?
if males_global_avg > females_global_avg:
    print(f"Chłopcy (mężczyźni) oceniają wyżej: {males_global_avg:.2f} vs {females_global_avg:.2f}")
else:
    print(f"Dziewczyny (kobiety) oceniają wyżej: {females_global_avg:.2f} vs {males_global_avg:.2f}")

# Zmiana typu danych jednej z kolumn na long
ratingsDf = ratingsDf.withColumn("total_votes", col("total_votes").cast("long"))
ratingsDf.show()

# zadanie Spark UI
# Zadanie 2 - Opis Spark UI
spark.sparkContext.uiWebUrl
# - Jobs: Monitorowanie zadań
# - Stages: Szczegóły etapów obliczeń
# - Storage: Pamięć używana przez RDD
# - Environment: Zmienne środowiskowe
# - Executors: Wykorzystanie zasobów

# 1. Jobs: Monitorowanie zadań
# W sekcji Jobs wyświetlane są wszystkie uruchomione zadania w ramach aplikacji Spark.
# Dla każdego zadania dostępne są informacje o stanie (np. succeeded, failed), czasach rozpoczęcia i zakończenia oraz liczbie przetworzonych danych.
# Sekcja ta umożliwia śledzenie postępu zapytań oraz diagnozowanie problemów z wydajnością.

# 2. Stages: Szczegóły etapów obliczeń
# Sekcja Stages dostarcza szczegółowe informacje o etapach obliczeń, które są częścią zadań w Spark.
# Każdy etap obejmuje operacje takie jak filtrowanie, agregowanie, łączenie itp.
# Wyświetlane są czasy trwania poszczególnych etapów, liczba przetworzonych wierszy oraz ewentualne błędy.
# Pomaga to w analizie etapów wymagających najwięcej zasobów lub czasu.

# 3. Storage: Pamięć używana przez RDD
# W sekcji Storage prezentowane są dane w pamięci, które zostały zapisane w ramach operacji RDD (Resilient Distributed Dataset).
# Podane są informacje o ilości zajmowanej pamięci przez poszczególne RDD, formie przechowywania (np. RAM, dysk) oraz wykonywanych na nich operacjach.
# Sekcja ta wspomaga optymalizację aplikacji poprzez identyfikowanie danych zajmujących nadmierną ilość pamięci.

# 4. Environment: Zmienne środowiskowe
# W sekcji Environment znajdują się szczegóły dotyczące konfiguracji środowiska Spark, takie jak zmienne środowiskowe, konfiguracje Spark (np. pamięć przydzielona dla executorów, liczba rdzeni), wersja Sparka oraz inne parametry konfiguracyjne.
# Może to wspierać proces debugowania oraz dostosowywania ustawień w celu poprawy wydajności.

# 5. Executors: Wykorzystanie zasobów
# Sekcja Executors przedstawia informacje o wykorzystywaniu zasobów obliczeniowych, w tym dane o każdym executorze, liczbie wykonanych zadań, czasie CPU, czasie I/O, pamięci używanej przez exekutorów oraz liczbie przetworzonych wierszy.
# Sekcja ta pozwala zidentyfikować nierównomierne obciążenie w klastrze lub problemy z pamięcią u niektórych executorów.



# zadanie 5 grouby

actorsDf.explain(True)
df_grouped = actorsDf.groupBy("name").agg(sum('age').alias("sum of age by people with the same name"))
df_grouped.explain(True)