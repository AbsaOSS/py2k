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
from typing import Any, Dict, List, Type

import pandas as pd
from pydantic import BaseModel

from py2k.creators import PandasModelCreator
from py2k.utils import (process_properties,
                        update_optional_schema)


class IterableAdapter:
    def __init__(self, iterator_factory):
        self.iterator_factory = iterator_factory

    def __iter__(self):
        return self.iterator_factory()


class KafkaModel(BaseModel):
    __key_fields__ = None

    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> List['KafkaModel']:
        records = df.to_dict('records')

        if records:
            return [cls(**item) for item in records]

        warnings.warn(
            "Unable to create kafka model from an empty dataframe.")
        return []

    @classmethod
    def iter_from_pandas(cls, df: pd.DataFrame):
        def iter_pandas(cls, df: pd.DataFrame):
            for record in df.to_dict('records'):
                yield cls(**record)
        return IterableAdapter(lambda: iter_pandas(cls, df))

    class Config:
        json_encoders = {
            datetime.date: lambda v: str(v),
            datetime.datetime: lambda v: str(v),
        }

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['KafkaModel']) -> None:
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
    def schema_from_iter(iterator: IterableAdapter):
        return list(itertools.islice(iterator, 1))[0].schema_json()

    def value_to_avro_dict(self):
        key_fields = set(self.key_fields) if self.key_fields else {}
        return json.loads(self.json(exclude=key_fields))

    def key_to_avro_dict(self):
        if not self.key_fields:
            return None

        return json.loads(self.json(include=set(self.key_fields)))

    @property
    def key_fields(self):
        return self.__key_fields__


class DynamicKafkaModel:
    """ class model for automatic serialization of Pandas DataFrame to
    KafkaModel
    """

    def __init__(self, df: pd.DataFrame, model_name: str,
                 fields_defaults: Dict[str, object] = None,
                 types_defaults: Dict[object, object] = None,
                 optional_fields: List[str] = None,
                 key_fields: List[str] = None):
        """
        Args:
            df (pd.DataFrame): Pandas dataframe to serialize
            model_name (str): destination Pydantic model
            fields_defaults (Dict[str, object], optional): default values for
                 fields in the dataframe. The keys are the fields names.
                 Defaults to None.
            types_defaults (Dict[object, object], optional): default values
                 for the types in the dataframe. The keys are the types,
                 e.g. int. Defaults to None.
            optional_fields (List[str], optional): list of fields which should
                 be marked as optional. Defaults to None.
            key_fields (List[str], optional): list of fields which are meant
                to be key of the schema
        """

        self._df = df
        _class = self._class(key_fields)

        model_creator = PandasModelCreator(df, model_name, fields_defaults,
                                           types_defaults, optional_fields,
                                           _class)

        self._model = model_creator.create()

    def from_pandas(self, df: pd.DataFrame = None) -> List['KafkaModel']:
        """create list of KafkaModel objects from a pandas DataFrame

        Args:
            df (pd.DataFrame): Pandas dataframe. Defaults to None.

        Returns:
            [List[KafkaModel]]: serialized list of KafkaModel objects
        """
        if df is not None:
            return self._model.from_pandas(df)

        return self._model.from_pandas(self._df)

    @staticmethod
    def _class(key_fields):
        if not key_fields:
            return KafkaModel

        class WithKey(KafkaModel):
            __key_fields__ = key_fields

        return WithKey
