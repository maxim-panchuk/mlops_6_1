from pyspark.sql import SparkSession
from src.config import Config
from src.logger import Logger
from src.preprocessor import Preprocessor
from clickhouse_connect import get_client
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline as PysparkPipeline
from pyspark.ml.evaluation import ClusteringEvaluator

class Pipeline:
    def __init__(
        self,
    ):
        # config
        self.config = Config()

        # logger
        log_level = self.config.get_logging_config()['level']
        self.Logger = Logger(
            name='pipeline',
            level=log_level,
        )

        # features
        self.features = [
            'energy-kcal_100g', 
            'fat_100g', 
            'saturated-fat_100g', 
            'carbohydrates_100g', 
            'sugars_100g', 
            'proteins_100g', 
            'fiber_100g', 
            'salt_100g'
        ]

        # spark
        spark_memory = self.config.get_spark_config()['spark.driver.memory']
        jdbc_path = self.config.get_spark_config()['spark.jars']
        self.spark = SparkSession.builder \
            .master('local[*]') \
            .config('spark.driver.memory', spark_memory) \
            .config("spark.jars", jdbc_path) \
            .getOrCreate()
        
        # preprocessor
        path_to_csv = self.config.get_pipeline_config()['path_to_csv']
        self.preprocessor = Preprocessor(
            path_to_csv=path_to_csv,
            spark=self.spark,
            features=self.features
        )

        # clickhouse client
        self.client = get_client(host='clickhouse', port=8123, username='mluser', password='superpass')

    def preprocess_raw_data(self):
        return self.preprocessor.preprocess()
    
    def save_data_clickhouse(self, df):
        pdf = df.toPandas()
        pdf.columns = [col.replace('-', '_') for col in pdf.columns]
        self.client.command("""
            CREATE TABLE IF NOT EXISTS food_features (
                energy_kcal_100g Float32,
                fat_100g Float32,
                saturated_fat_100g Float32,
                carbohydrates_100g Float32,
                sugars_100g Float32,
                proteins_100g Float32,
                fiber_100g Float32,
                salt_100g Float32
            ) ENGINE = MergeTree()
            ORDER BY tuple()
        """)
        self.client.command("TRUNCATE TABLE food_features")
        self.client.insert_df('food_features', pdf)
        self.Logger.info('data saved to clickhouse')

    def read_data_from_clickhouse(self):
        config = self.config.get_clickhouse_config()

        df_clickhouse = self.spark.read \
            .format("jdbc") \
            .option("url", config['jdbc_url']) \
            .option("dbtable", config['table_name']) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .option("driver", config['driver']) \
            .load()
    
        self.Logger.info('data from clickhouse read')
    
        return df_clickhouse
    
    def clasterize(self, df):
        features = [col.replace('-', '_') for col in self.features]

        assembler = VectorAssembler(
            inputCols=features,
            outputCol="features_raw"
        )

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True
        )

        CLUSTER_COUNT = 5
        seed = 42

        kmeans = KMeans(k=CLUSTER_COUNT, seed=seed, featuresCol="features")

        pipeline = PysparkPipeline(stages=[assembler, scaler, kmeans])

        model = pipeline.fit(df)
        self.Logger.info("model successfully fitted!")

        kmeans_model = model.stages[-1]

        centers = kmeans_model.clusterCenters()
        print("Cluster Centers (scaled):")
        for i, c in enumerate(centers):
            print(f"Cluster {i}:", c)

        predictions = model.transform(df)

        return kmeans_model, predictions
    
    def metrics(self, kmeans_model, predictions):
        self.Logger.info(f'cluster sizes: {kmeans_model.summary.clusterSizes}')
        evaluator = ClusteringEvaluator()

        silhouette = evaluator.evaluate(predictions)
        self.Logger.info(f"Silhouette Score = {silhouette:.4f}")

    def save_model(self, model):
        path_to_model = self.config.get_pipeline_config()['path_to_model']
        model.save(path_to_model)
        self.Logger.info(f'model saved!')

    def start(self):
        df = self.preprocess_raw_data()
        self.save_data_clickhouse(df)
        df = self.read_data_from_clickhouse()
        kmeans_model, predictions = self.clasterize(df)
        self.metrics(kmeans_model, predictions)
        self.Logger.info('pipeline finished')
        