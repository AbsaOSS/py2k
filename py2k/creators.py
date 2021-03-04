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

import warnings
from abc import ABC
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional

import pandas as pd
from pydantic import Field, create_model, BaseModel


class ModelCreator(ABC):
    def create(self):
        ...


class PandasModelCreator(ModelCreator):
    """
    Dynamically converts a Pandas dataframe to a Pydantic model.

    The fields and types defaults can be used together, i.e. it is possible to specify some defaults by field
        name and some defaults by type.

    Currently, only strings, integers and floats are accepted.

    If, for a given field, default values are not found either for its name nor for its type, a default per type
        will be used.

    The defaults per type are:
        str = ""
        int = -1
        float = -1.0
        Decimal not directly supported, will be converted to float, so the default same as float (always)
        bool = False
        date = date.min
        pd.Timestamp = pd.Timestamp.min - Use when you need to set default datetime

    They can be found in KafkaModel._SCHEMA_TYPES_DEFAULTS

    :param df: Pandas dataframe
    :param model_name: destination Pydantic model
    :param fields_defaults: default values for fields in the dataframe. The keys are the fields names
    :param types_defaults: default values for the types in the dataframe. The keys are the types, e.g. int
    :param optional_fields: list of fields which should be marked as optional
    :return: Records from the Pandas dataframe parsed as instances of KafkaModel
    """

    _SCHEMA_TYPES_DEFAULTS = {
        str: "",
        int: -1,
        float: -1.0,
        bool: False,
        Decimal: -1.0,
        date: date.min,
        pd.Timestamp: pd.Timestamp.min,
    }

    def __init__(self,
                 df: pd.DataFrame,
                 model_name: str,
                 fields_defaults: Dict[str, object] = None,
                 types_defaults: Dict[object, object] = None,
                 optional_fields: List[str] = None,
                 base: type = BaseModel):
        self._df = df
        self._model_name = model_name
        self._fields_defaults = fields_defaults
        self._types_defaults = types_defaults
        self._optional_fields = optional_fields
        self._base = base

    def create(self):
        records_as_dict_list = self._df.to_dict('records')
        if records_as_dict_list:
            sample_record = records_as_dict_list[0]
            model = self._create_model(sample_record)
            return self._to_model(model, records_as_dict_list)
        else:
            warnings.warn(
                "Unable to create kafka model from an empty dataframe.")
            return []

    def _create_model(self, record):
        def field_definition(field_name, field_value):
            """
            Creates a field definition containing its type, title and default value.
            :return: tuple like (field type, Field(title, default value)
            """

            value_type = type(field_value)

            if self._fields_defaults and field_name in self._fields_defaults:
                # if there is a value_default for the field, use it
                value_default = self._fields_defaults.get(field_name)
            else:
                # if there is preferred value_default for the type, use it
                if self._types_defaults and value_type in self._types_defaults:
                    value_default = self._types_defaults.get(value_type)
                    # otherwise, use the value_default value_default for the type
                elif value_type in self._SCHEMA_TYPES_DEFAULTS:
                    value_default = self._SCHEMA_TYPES_DEFAULTS.get(value_type)
                else:
                    raise ValueError(f"Invalid type for field '{field_name}': '{value_type}'. "
                                     f"Supported types: '{self._SCHEMA_TYPES_DEFAULTS.keys()}'")

            # keep the title the same as the field name since the title is the value used when generating the Json
            # schema from the record, which is also used to generate the Avro schema.
            #
            # If this is not done, the title will be different from the field name when the Avro serializer is producing
            # the records, because the title uses 'snake case', therefore, there will be no matching between the fields
            # which will take the serializer to assume the default values only
            if self._optional_fields and field_name in self._optional_fields:
                value_type = Optional[value_type]

            return value_type, Field(title=field_name, default=value_default)

        if self._types_defaults and self._types_defaults.get(float):
            self._types_defaults[Decimal] = self._types_defaults.get(float)

        template = {field_name: field_definition(
            field_name, field_value) for field_name, field_value in record.items()}
        return create_model(self._model_name, **template, __base__=self._base)

    def _to_model(self, model, records):
        class Wrapper(self._base):
            __root__: List[model]

        return Wrapper.parse_obj(records).__root__
