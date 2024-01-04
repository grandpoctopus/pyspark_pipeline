from abc import ABC
from typing import Dict, List

from pydantic import BaseModel, create_model
from pyspark.sql import DataFrame


class HiveSchema(BaseModel, ABC):
    """
    Abstract Base Class for pydantic models
    representing Hive Schemas. Contains method for
    deserializing the schema for Pyspark and Hive functions
    """

    def get_columns_list(self) -> List:
        """Get column names as a list of strings"""
        return list(self.dict().keys())

    def set_dict_to_struct_string(self, struct: Dict) -> None:
        """
        Creates a struct string for SQL create statements from an
        EventsSchema object
        """
        for k, v in struct.items():
            if type(v) == dict:
                self.set_dict_to_struct_string(v)
                inner_string = ", ".join(
                    f"{column}:{data_type}" for column, data_type in v.items()
                )
                struct[k] = f"STRUCT<{inner_string}>"
            if type(v) == list:
                if type(v[0]) == dict:
                    self.set_dict_to_struct_string(v[0])
                    inner_string = ", ".join(
                        f"{column}:{data_type}"
                        for column, data_type in v[0].items()
                    )
                    array_type = f"STRUCT<{inner_string}>"
                else:
                    array_type = v[0]
                struct[k] = f"ARRAY<{array_type}>"

    def get_schema_string(self) -> str:
        """
        Creates a string for SQL create statements from an
        EventsSchema object

        Pydantic.BaseModel.dict() method converts a pydantic
        model to a dict where attributes are keys with their
        corresponding values. In cases where a value is
        another pydantic object these will rendered as
        a nested dict.

        set_dict_to_struct_string() will recursively
        traverse a nested pydantic dict and convert
        each nested dict/list to a string representing
        the dict/list as a SQL struct or array as appropriate
        """
        schema_dict = self.dict()
        self.set_dict_to_struct_string(schema_dict)

        return ", ".join(
            f"{column} {data_type}"
            for column, data_type in schema_dict.items()
            if data_type is not None
        )


class EtlSchema(HiveSchema):
    """
    A dataclass for table schemas for Hive tables
    """

    id = "bigint"


class AuditSchema(HiveSchema):
    """
    A dataclass for table schemas for Hive tables
    """

    run_id = "string"
    run_timestamp = "string"
    include_start_date = "string"
    include_end_date = "string"
    incremental_load_start_date = "string"
    incremental_load_end_date = "string"
    settings = "string"


class CountKeysSchema(HiveSchema):
    """
    Schema for counting keys query
    """

    key = "string"
    count = "bigint"


def SchemaFactory(schema_name, fields: Dict, base_class=HiveSchema):
    """
    Dynamically creates schema models
    """
    return create_model(
        schema_name,
        __base__=base_class,
        **fields,
    )


def DataFrameSchemaFactory(schema_name, df: DataFrame, base_class=HiveSchema):
    """
    Dynamically creates schema models
    """
    return SchemaFactory(
        schema_name,
        dict(df.dtypes),
        base_class,
    )
