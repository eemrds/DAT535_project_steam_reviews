# Game Recommendation with pyspark and Hadoop

## How to run
To run the project there are two different files that are important. `steam_reviews.ipynb` and `preprocess.ipynb`

1. Download the dataset from [Kaggle](https://www.kaggle.com/datasets/smeeeow/steam-game-reviews)
2. Make sure Hadoop and Spark is set up and configured.
3. Upload the csv files to Hadoop cluster
    * hdfs dfs -mkdir /steam_reviews
    * hdfs dfs -mkdir /steam_reviews/csvs
    * hdfs dfs -put steam_reviews_csvs/*.csv /steam_reviews/csvs
4. Run `preprocess.ipynb`
5. Run `steam_reviews.ipynb`