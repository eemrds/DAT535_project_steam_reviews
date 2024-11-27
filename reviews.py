import findspark
import pyspark
import pyspark.sql
from delta import *
from pyspark import RDD
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, udf
from pyspark.sql.types import FloatType, IntegerType
from tqdm.notebook import tqdm

findspark.init()


DELTA_TABLE = "hdfs:///steam_reviews/steam_reviews_processed"


class SteamReviews:
    """Class containing methods for interacting with Steam reviews."""

    def __init__(
        self, name: str = "SteamReviews", delta_path: str = DELTA_TABLE
    ) -> None:
        """Initialize the SteamReviews instance."""
        self.name = name
        self.delta_path = delta_path
        self.spark = self.get_spark()
        self.review = self.load_data()
        self.model = self.train_als_model()  # Train ALS model

    def get_spark(self) -> SparkSession:
        """Creates the Spark session."""
        print("--- Creating Spark session ---")
        builder = (
            pyspark.sql.SparkSession.builder.appName(self.name)
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def load_data(self) -> pyspark.sql.DataFrame:
        """Loads the data from the delta table."""
        print("--- Loading delta table ---")
        review = self.spark.read.format("delta").load(DELTA_TABLE)
        print("--- Loaded delta table ---")
        return review

    def get_distinct(self, column: str = "game") -> list[str]:
        """Retrieves all unique games."""
        return [g["game"] for g in self.review.select(column).distinct().collect()]

    def get_distinct_sorted(self, column: str = "game", asc: bool = False) -> list[str]:
        """Returns the values sorted by occurence."""
        print(
            f"--- Getting unique values for {column} sorted {'asc' if asc else 'desc'} ---"
        )
        distinct_and_occurence = self.review.groupBy(column).count()
        distinct_sorted = sorted(
            distinct_and_occurence.collect(), key=lambda game: game[1], reverse=not asc
        )
        return [(g["game"], g["count"]) for g in distinct_sorted]

    def get_reviews(self, game: str) -> list[dict]:
        """Get reviews relating to a game."""
        return self.review.filter(self.review["game"] == game).collect()

    def get_top_overall_rated_games(self, asc: bool = False) -> list[str]:
        """Get top overall rated games by positive review percentage, sorted ascending or descending."""
        # Aggregate total reviews and positive reviews for each game
        game_reviews_df = self.review.groupBy("game").agg(
            pyspark.sql.functions.count("*").alias("total_reviews"),
            pyspark.sql.functions.sum(
                pyspark.sql.functions.when(col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
        )

        # Calculate the positive review percentage for each game
        game_reviews_df = game_reviews_df.withColumn(
            "positive_percentage",
            (col("positive_reviews") / col("total_reviews")) * 100,
        )

        # Sort by positive percentage
        top_rated_games_df = game_reviews_df.orderBy(
            col("positive_percentage").asc()
            if asc
            else col("positive_percentage").desc()
        )

        # Collect the top-rated games and return their names
        top_rated_games = [row["game"] for row in top_rated_games_df.collect()]
        return top_rated_games

    def get_game_name_from_appid(self, appid: int) -> str:
        """Get the game name from an appid."""
        game_row = (
            self.review.filter(col("appid") == appid).select("game").distinct().first()
        )
        return game_row["game"] if game_row else "Unknown Game"

    def get_game_recommendations(self, current_game: str, top_n: int = 3) -> list[str]:
        """Get game recommendations based on positive reviews of the current game."""
        # Get users who positively rated the current game
        positive_users = (
            self.review.filter(
                (col("game") == current_game) & (col("voted_up") == True)
            )
            .select("author_steamid")
            .distinct()
        )
        # Find other games these users positively reviewed
        other_games = (
            self.review.join(positive_users, on="author_steamid", how="inner")
            .filter((col("game") != current_game) & (col("voted_up") == True))
            .groupBy("game")
            .count()
            .orderBy(col("count").desc())
        )
        # Get the top N recommended games
        recommended_games = [row["game"] for row in other_games.take(top_n)]

        return recommended_games

    def train_als_model(self):
        """Trains the ALS model on the review data."""
        recommend_df = (
            self.review.select(
                ["author_steamid", "appid", "voted_up", "author_playtime_forever"]
            )
            .withColumn("author_steamid", col("author_steamid").cast("integer"))
            .withColumn("appid", col("appid").cast("integer"))
            .withColumn("voted_up", col("voted_up").cast("integer"))
            .withColumn(
                "author_playtime_forever",
                col("author_playtime_forever").cast("integer"),
            )
        )

        recommend_df = recommend_df.join(
            recommend_df.select(["author_steamid", "author_playtime_forever"])
            .groupBy("author_steamid")
            .agg(
                min("author_playtime_forever").alias("min_playtime"),
                max("author_playtime_forever").alias("max_playtime"),
            ),
            on="author_steamid",
            how="left",
        )

        recommend_df.withColumn(
            "score",
            col("voted_up")
            * (
                (col("author_playtime_forever") - col("min_playtime"))
                / (1 + col("max_playtime") - col("min_playtime"))
            ),
        )

        train, test = recommend_df.randomSplit([0.6, 0.4])

        recommender = ALS(
            userCol="author_steamid",
            itemCol="appid",
            ratingCol="score",
            coldStartStrategy="drop",
        )

        print("--- Training ALS model ---")
        model = recommender.fit(train)
        print("--- ALS model trained ---")

        return model

    def get_user_recommendations(self, author_steamid: int) -> list[str]:
        """Get game recommendations for a specific user based on ALS model."""
        # Get recommendations for all users
        user_recommendations = self.model.recommendForAllUsers(3)

        # Filter to get recommendations for the specific user
        recommendations = (
            user_recommendations.filter(col("author_steamid") == author_steamid)
            .select("recommendations")
            .collect()
        )

        if not recommendations:
            return []

        # The 'recommendations' column contains a list of structs with 'appid' and 'rating'
        recommended_appids = [
            row["appid"]
            for rec in recommendations[0]["recommendations"]
            for row in [rec]
        ]
        recommended_games = [
            self.get_game_name_from_appid(appid) for appid in recommended_appids
        ]

        return recommended_games


steam = SteamReviews()

""" recommend_df = (
    steam.review.select(
        ["author_steamid", "appid", "voted_up", "author_playtime_forever"]
    )
    .withColumn("author_steamid", col("author_steamid").cast("integer"))
    .withColumn("appid", col("appid").cast("integer"))
    .withColumn("voted_up", col("voted_up").cast("integer"))
    .withColumn(
        "author_playtime_forever", col("author_playtime_forever").cast("integer")
    )
)

recommend_df = recommend_df.join(
    recommend_df.select(["author_steamid", "author_playtime_forever"])
    .groupBy("author_steamid")
    .agg(
        min("author_playtime_forever").alias("min_playtime"),
        max("author_playtime_forever").alias("max_playtime"),
    ),
    on="author_steamid",
    how="left",
)

recommend_df = recommend_df.withColumn(
    "score",
    (col("author_playtime_forever") - col("min_playtime"))
    / (1 + col("max_playtime") - col("min_playtime")),
)

train, test = recommend_df.randomSplit([0.6, 0.4])

recommender = ALS(
    userCol="author_steamid",
    itemCol="appid",
    ratingCol="score",
    coldStartStrategy="drop",
)

model = recommender.fit(train)

predictions = model.transform(test)

rec = model.recommendForAllUsers(3) """
