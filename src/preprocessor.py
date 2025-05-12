from pyspark.sql.functions import col, lit
from functools import reduce
from src.logger import Logger

class Preprocessor:
    def __init__(
        self,
        path_to_csv: str,
        spark,
        features,
    ):
        self.path_to_csv = path_to_csv
        self.spark = spark
        self.features = features
        self.logger = Logger("preprocessor")

    def load_from_scv(self):
        df = self.spark.read.option("delimiter", "\t") \
            .csv(self.path_to_csv, header=True, inferSchema=True)
        self.logger.info("loaded from csv")
        return df
    
    def select_features(self, df):
        df = df.select(*self.features)
        self.logger.info("selected features")
        return df
    
    def drop_na_columns(self, df):
        df = df.na.drop()
        self.logger.info("dropped na columns")
        return df
    
    def set_df_limit(self, df, limit):
        df = df.limit(limit)
        self.logger.info(f"df limit set to {limit}")
        return df
    
    def filter_df(self, df):
        columns_to_check = [c for c in self.features if c != 'energy-kcal_100g']
        threshold = 100
        max_kcal_per_100g = 900

        filter_condition = reduce(
            lambda acc, c: acc & (col(c) < threshold),
            columns_to_check,
            lit(True)
        )

        energy_condition = col("energy-kcal_100g") < max_kcal_per_100g

        positive_condition = reduce(
            lambda acc, c: acc & (col(c) > 0),
            columns_to_check + ["energy-kcal_100g"],
            lit(True)
        )

        df = df.filter(
            filter_condition &
            energy_condition &
            positive_condition
        )

        self.logger.info("df filtered successfully!")
        return df
    
    def preprocess(self):
        df = self.load_from_scv()
        df = self.select_features(df)
        df = self.drop_na_columns(df)
        df = self.set_df_limit(df, 300000)
        df = self.filter_df(df)
        self.logger.info("preprocessing successfully finished!")
        return df