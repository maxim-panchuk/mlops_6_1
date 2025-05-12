from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline as PysparkPipeline
from pyspark.sql import SparkSession
from src.preprocessor import Preprocessor
from src.config import Config
from src.logger import Logger
from pyspark.ml.evaluation import ClusteringEvaluator

class Pipeline:
    def __init__(
        self,
    ):
        self.config = Config()
        config_level = self.config.get_logging_config()['level']

        self.logger = Logger(
            name="pipeline",
            level=config_level,
        )

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

        spark_memory = self.config.get_spark_config()['spark.driver.memory']
        self.spark = SparkSession.builder \
            .master("local[*]") \
            .config("spark.driver.memory", spark_memory) \
            .getOrCreate()

        path_to_csv = self.config.get_pipeline_config()['path_to_csv']
        self.preprocessor = Preprocessor(
            path_to_csv=path_to_csv,
            spark=self.spark,
            features=self.features
        )
    
    def start_pipeline(self):

        assembler = VectorAssembler(
            inputCols=self.features,
            outputCol="features_raw"
        )

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True
        )

        df = self.preprocessor.preprocess()

        CLUSTER_COUNT = 5
        seed = 42

        kmeans = KMeans(k=CLUSTER_COUNT, seed=seed, featuresCol="features")

        pipeline = PysparkPipeline(stages=[assembler, scaler, kmeans])

        model = pipeline.fit(df)
        self.logger.info("model successfully fitted!")

        kmeans_model = model.stages[-1]

        centers = kmeans_model.clusterCenters()
        print("Cluster Centers (scaled):")
        for i, c in enumerate(centers):
            print(f"Cluster {i}:", c)

        predictions = model.transform(df)

        self.metrics(kmeans_model, predictions)
    
    def metrics(self, kmeans_model, predictions):
        self.logger.info(f'cluster sizes: {kmeans_model.summary.clusterSizes}')
        evaluator = ClusteringEvaluator()

        silhouette = evaluator.evaluate(predictions)
        self.logger.info(f"Silhouette Score = {silhouette:.4f}")

    def save_model(self, model):
        path_to_model = self.config.get_pipeline_config()['path_to_model']
        model.save(path_to_model)
        self.logger.info(f"model saved!")

        