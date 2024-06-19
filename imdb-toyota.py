import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("IMDb Analysis").config("spark.driver.memory", "8g").config("spark.executor.memory", "8g").getOrCreate()

# Load the data into DataFrames
title_basics_large_df = spark.read.option("compression", "gzip").csv("imdb-datasets/large/title.basics.tsv.gz", sep="\t", header=True)
title_ratings_df = spark.read.option("compression", "gzip").csv("imdb-datasets/large/title.ratings.tsv.gz", sep="\t", header=True)
title_principals_df = spark.read.option("compression", "gzip").csv("imdb-datasets/large/title.principals.tsv.gz", sep="\t", header=True)
name_basics_df = spark.read.option("compression", "gzip").csv("imdb-datasets/large/name.basics.tsv.gz", sep="\t", header=True)

# Filter title basics to only include movies
title_basics_df = title_basics_large_df.filter(col("titleType") == "movie")

# Filter only movies with at least 500 votes
title_ratings_min_500_votes_df = title_ratings_df.filter(col("numVotes") >= 500)

# Join the title_basics_df with title_ratings_min_500_votes_df to get the average rating for movies only
title_ratings_min_500_votes_df = title_ratings_min_500_votes_df.join(
    title_basics_df,
    title_ratings_min_500_votes_df.tconst == title_basics_df.tconst,
    "inner"
).select(title_ratings_min_500_votes_df.tconst,
         title_ratings_min_500_votes_df.averageRating,
         title_ratings_min_500_votes_df.numVotes)

# Calculate the average number of votes per movie
avg_votes = title_ratings_min_500_votes_df.select(avg("numVotes")).first()[0]

# Calculate the ranking score for each movie
ranked_movies_df = title_ratings_min_500_votes_df.select(
    col("tconst"),
    ((col("numVotes") / avg_votes) * col("averageRating")).alias("rankingScore")
)

# 1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
# (numVotes/averageNumberOfVotes) * averageRating
top_10_movies_df = ranked_movies_df.orderBy(col("rankingScore").desc()).limit(10)
top_10_movies_df = top_10_movies_df.withColumnRenamed("tconst", "movie_tconst")

# Join the top 10 movies with the title_basics_df to get the movie titles
top_10_movies_with_titles_df = top_10_movies_df.join(
    title_basics_df,
    top_10_movies_df.movie_tconst == title_basics_df.tconst,
    "inner"
)

# Display the top 10 movies with their ranking score
print("Top 10 movies with a minimum of 500 votes with the ranking determined by: (numVotes/averageNumberOfVotes) * averageRating")
top_10_movies_for_display_df = top_10_movies_with_titles_df.select("primaryTitle", "rankingScore").orderBy(col("rankingScore").desc())
top_10_movies_for_display_df.show()

# 2. For these 10 movies, list the persons who are most often credited and list the
# different titles of the 10 movies.

title_principals_df = title_principals_df.withColumnRenamed("tconst", "principal_tconst")
# Join the top 10 movies with the title_principals_df to get the principals of the movies
top_10_movies_with_principals_df = top_10_movies_with_titles_df.join(
    title_principals_df,
    top_10_movies_with_titles_df.movie_tconst == title_principals_df.principal_tconst,
    "inner"
)

# Persist the top 10 movies with principals to a table for nested queries
warehouse_dir = "spark-warehouse"
table_dir = "top_10_movies_with_principals"
table_path = os.path.join(warehouse_dir, table_dir)
if os.path.exists(table_path):
    import shutil
    shutil.rmtree(table_path)
top_10_movies_with_principals_df.write.mode("overwrite").saveAsTable("top_10_movies_with_principals")

# Get the top 10 movies with the maximum number of person credits
top_10_movies_with_max_nconst_credits_df = spark.sql("""
    SELECT t1.movie_tconst, t1.nconst, t1.number_of_credits
    FROM (
        SELECT movie_tconst, nconst, COUNT(nconst) AS number_of_credits,
               RANK() OVER (PARTITION BY movie_tconst ORDER BY COUNT(nconst) DESC) AS rnk
        FROM top_10_movies_with_principals
        GROUP BY movie_tconst, nconst
    ) t1
    WHERE t1.rnk = 1
""")

# Join top_10_movies_with_max_nconst_credits_and_titles_df with top_10_movies_with_titles_df to get the movie titles
top_10_movies_with_max_nconst_credits_and_titles_df = top_10_movies_with_max_nconst_credits_df.join(
    top_10_movies_with_titles_df,
    top_10_movies_with_max_nconst_credits_df.movie_tconst == top_10_movies_with_titles_df.movie_tconst,
    "inner"
).select(top_10_movies_with_titles_df.primaryTitle, top_10_movies_with_max_nconst_credits_df.nconst, top_10_movies_with_titles_df.rankingScore, top_10_movies_with_max_nconst_credits_df.number_of_credits)

# Join top_10_movies_with_max_nconst_credits_and_titles_df with name_basics_df to get the names of the persons
top_10_movies_with_max_nconst_credits_and_titles_and_names_df = top_10_movies_with_max_nconst_credits_and_titles_df.join(
    name_basics_df,
    top_10_movies_with_max_nconst_credits_and_titles_df.nconst == name_basics_df.nconst,
    "inner"
).select(top_10_movies_with_max_nconst_credits_and_titles_df.primaryTitle, name_basics_df.primaryName, top_10_movies_with_max_nconst_credits_and_titles_df.rankingScore, top_10_movies_with_max_nconst_credits_and_titles_df.number_of_credits).orderBy(col("rankingScore").desc())

print("Top ten movies with the names of persons who are most often credited to those movies")
top_10_movies_with_max_nconst_credits_and_titles_and_names_df.show(500)

spark.stop()