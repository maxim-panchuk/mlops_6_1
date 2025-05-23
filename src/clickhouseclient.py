
from src.config import Config
from src.logger import Logger
from clickhouse_connect import get_client

class ClickHouseClient:
    def __init__(
        self,
        spark,
    ):
        self.spark = spark

        self.config = Config().get_clickhouse_config()

        self.Logger = Logger(
            name='pipeline',
            level=Config().get_logging_config()['level'],
        )

        self.client = get_client(
            host=self.config['host'],
            port=self.config['port'],
            username=self.config['user'],
            password=self.config['password'],
        )

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
        df_clickhouse = self.spark.read \
            .format('jdbc') \
            .option('url', self.config['jdbc_url']) \
            .option('dbtable', self.config['table_name']) \
            .option('user', self.config['user']) \
            .option('password', self.config['password']) \
            .option('driver', self.config['driver']) \
            .load()
        
        self.Logger.info('data from clickhouse read')

        return df_clickhouse

