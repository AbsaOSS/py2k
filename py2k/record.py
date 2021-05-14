# Copyright 2021 ABSA Group Limited

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import itertools
import json
import warnings
from copy import deepcopy
from typing import Any, Dict, List, Type, Set

import pandas as pd
from pydantic import BaseModel

from py2k.creators import PandasModelCreator
from py2k.utils import (process_properties,
                        update_optional_schema)


class KafkaRecord(BaseModel):
    __key_fields__: set = {}
    __include_key__: bool = False

    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> List['KafkaRecord']:
        records = df.to_dict('records')

        if records:
            return [cls(**item) for item in records]

        warnings.warn(
            "Unable to create kafka model from an empty dataframe.")
        return []

    @classmethod
    def iter_from_pandas(cls, df: pd.DataFrame):
        for record in df.to_dict('records'):
            yield cls(**record)

    class Config:
        json_encoders = {
            datetime.date: lambda v: str(v),
            datetime.datetime: lambda v: str(v),
        }

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['KafkaRecord']) -> None:
            schema['type'] = 'record'
            schema['name'] = schema.pop('title')
            schema['namespace'] = (f'python.kafka.'
                                   f'{schema["name"].lower()}')
            schema = process_properties(schema)
            schema.pop('properties')

            # Dynamically generated schemas might not have this field,
            # which is removed anyway.
            if 'required' in schema:
                schema.pop('required')
            update_optional_schema(schema=schema, model=model)

    @staticmethod
    def schema_from_iter(iterator):
        return list(itertools.islice(iterator, 1))[0].schema_json()

    def value_to_avro_dict(self):
        return self._to_avro_dict(exclude=self.key_fields)

    def key_to_avro_dict(self):
        if not self.key_fields:
            return None

        return self._to_avro_dict(include=self.key_fields)

    def _to_avro_dict(self, exclude=None, include=None):
        return json.loads(self.json(include=include, exclude=exclude))

    @property
    def key_fields(self):
        return self.__key_fields__

    @property
    def include_key(self):
        return self.__include_key__

    @property
    def value_fields(self):
        exclude = None
        if self.key_fields and not self.include_key:
            exclude = self.key_fields
        _dict = self.dict(exclude=exclude)
        return set(_dict.keys())

    @property
    def value_schema_string(self):
        schema = deepcopy(self.schema())
        schema['fields'] = self._filter_fields(schema['fields'],
                                               self.value_fields)
        return self._dict_to_str(schema)

    @property
    def key_schema_string(self):
        schema = deepcopy(self.schema())

        schema['name'] = f'{schema["name"]}Key'
        schema['fields'] = self._filter_fields(schema['fields'],
                                               self.key_fields)
        return self._dict_to_str(schema)

    @staticmethod
    def _filter_fields(fields: List[Dict[str, Any]], names: set):
        return [field for field in fields if field.get('name') in names]

    @staticmethod
    def _dict_to_str(dictionary):
        return str(dictionary).replace("'", "\"")


class PandasToRecordsTransformer:
    """ class model for automatic serialization of Pandas DataFrame to
    KafkaRecord
    """

    def __init__(self, df: pd.DataFrame, record_name: str,
                 fields_defaults: Dict[str, object] = None,
                 types_defaults: Dict[object, object] = None,
                 optional_fields: List[str] = None,
                 key_fields: Set[str] = None,
                 include_key: bool = None):
        """
        Args:
            df (pd.DataFrame): Pandas dataframe to serialize
            record_name (str): destination Pydantic model
            fields_defaults (Dict[str, object], optional): default values for
                 fields in the dataframe. The keys are the fields names.
                 Defaults to None.
            types_defaults (Dict[object, object], optional): default values
                 for the types in the dataframe. The keys are the types,
                 e.g. int. Defaults to None.
            optional_fields (List[str], optional): list of fields which should
                 be marked as optional. Defaults to None.
            key_fields (Set[str], optional): set of fields which are meant
                to be key of the schema
            include_key: bool: Indicator whether key fields should be
                included in value
        """

        self._df = df
        _class = self._class(key_fields, include_key)

        model_creator = PandasModelCreator(df, record_name, fields_defaults,
                                           types_defaults, optional_fields,
                                           _class)

        self._model = model_creator.create()

    def from_pandas(self, df: pd.DataFrame = None) -> List['KafkaRecord']:
        """Creates list of KafkaModel objects from a pandas DataFrame

        Args:
            df (pd.DataFrame): Pandas dataframe. Defaults to None.

        Returns:
            [List[KafkaModel]]: serialized list of KafkaModel objects

        Examples:
            >>> record_transformer = PandasToRecordsTransformer(df=df,
                                                                record_name='KafkaRecord')
            >>> record_transformer.from_pandas()
                [KafkaRecord(name='Daniel',
                             cool_level='low',
                             value=27.1,
                             date=datetime.date(2021, 3, 1))]

        """
        if df is not None:
            return self._model.from_pandas(df)

        return self._model.from_pandas(self._df)

    def iter_from_pandas(self, df: pd.DataFrame):
        """Creates iterator of KafkaModel objects from a pandas DataFrame

                Args:
                    df (pd.DataFrame): Pandas dataframe. Defaults to None.

                Returns:
                    [List[KafkaModel]]: serialized list of KafkaModel objects

                Examples:
                    >>> record_transformer = PandasToRecordsTransformer(df=df,
                                                                        record_name='KafkaRecord')
                    >>> record_transformer.iter_from_pandas()
                """
        if df is not None:
            return self._model.iter_from_pandas(df)

        return self._model.iter_from_pandas(self._df)

    @staticmethod
    def _class(key_fields, include_key):
        class NewRecord(KafkaRecord):
            if include_key is not None:
                __include_key__ = include_key
            if key_fields is not None:
                __key_fields__ = key_fields

        return NewRecord
