from pyspark import SparkConf
from pyspark.sql import SparkSession


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SparkInstance(metaclass=Singleton):
    session = None

    def __init__(self, app_name, conf):
        conf = SparkConf().setAll(conf)
        self.session = (SparkSession
                        .builder
                        .appName(app_name)
                        .config(conf=conf)
                        .getOrCreate())
        self.context = self.session.sparkContext
        self.context.setLogLevel("WARN")
