import findspark
findspark.init("/home/gtusr0070/Apache-Spark/spark-2.4.4-bin-hadoop2.7/")

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import logging
import json

class CEPExample(object):


    def __init__(self):
        self.get_kafka_streaming()
        super(CEPExample, self).__init__()

    def sparkContext(self, core, appName):
        _core_parameter = "local[" + str(core) + "]"
        return SparkContext(_core_parameter, appName=appName)

    def _create_streaming(self, appName, window_time, core="*"):
        sc = self.sparkContext(appName=appName, core=core)
        self.ssc = StreamingContext(sc, window_time)
        return self.ssc

    def _create_kafka_stream(self, ssc, zk, groupid, topics):
        self.kafka_stream = KafkaUtils.createStream(
            ssc = ssc,
            zkQuorum = zk,
            groupId = groupid,
            topics = topics
        )
        return self.kafka_stream

    def get_kafka_streaming(self):
        _ssc = self._create_streaming(appName="CEP", window_time=60)
        _kafka_ssc = self._create_kafka_stream(
            _ssc,
            "10.0.0.46:2181",
            "spark-streaming",
            {"crm-engine": 1}
        )
        parsed = _kafka_ssc.map(lambda v: json.loads(v[1]))
        parsed.pprint()
        self.ssc.start()
        self.ssc.awaitTermination() 

CEPExample() 
