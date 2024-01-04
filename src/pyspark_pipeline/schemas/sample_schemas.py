from pyspark_pipeline.schemas import EtlSchema


class SampleJoinSchema(EtlSchema):
    value = "bigint"


class SampleAggregationSchema(EtlSchema):
    max_value = "bigint"
