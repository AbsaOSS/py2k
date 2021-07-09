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

from abc import ABC
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import Field, create_model, BaseModel


class ModelCreator(ABC):
    def create(self):
        ...


class PandasModelCreator(ModelCreator):
    """Dynamically converts a Pandas dataframe to a Pydantic model.

    The fields and types defaults can be used together, i.e. it is possible
        to specify some defaults by field name and some defaults by type.

    Currently, only strings, integers and floats are accepted.

    If, for a given field, default values are not found either for its name
        nor for its type, a default per type will be used.

    The defaults per type are:
        - str = ""
        - int = -1
        - float = -1.0
        - Decimal not directly supported, will be converted to float,
            so the default same as float (always)
        - bool = False
        - date = date.min
        - pd.Timestamp = pd.Timestamp.min
            Use when you need to set default datetime

    They can be found in KafkaModel._SCHEMA_TYPES_DEFAULTS
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
        """
        Args:
            df (pd.DataFrame): Pandas dataframe
            model_name (str): destination Pydantic model
            fields_defaults (Dict[str, object], optional): default values for
                 fields in the dataframe. The keys are the fields names.
                 Defaults to None.
            types_defaults (Dict[object, object], optional): default values
                 for the types in the dataframe. The keys are the types,
                 e.g. int. Defaults to None.
            optional_fields (List[str], optional): list of fields which should
                 be marked as optional. Defaults to None.
            base (type, optional): Pydantic model to create.
                 Defaults to BaseModel.
        """
        self._df = df
        self._model_name = model_name
        self._fields_defaults = fields_defaults
        self._types_defaults = types_defaults
        self._optional_fields = optional_fields
        self._base = base

    def create(self):
        if self._df.empty:
            raise ValueError(
                "Unable to create kafka model from an empty dataframe.")

        sample_record = {
            col: self._sample_col(col) for col in self._df.columns
        }

        return self._create_model(sample_record)

    def _sample_col(self, col):
        return self._df[[col]]\
                .dropna()\
                .to_dict('records')[0][col]

    def _create_model(self, record):
        def field_definition(field_name: str, field_value: Any):
            """Creates a field definition containing its
                type, title and default value.
            Args:
                field_name ([str]): field name
                field_value ([Any]): field value

            Raises:
                ValueError: if schema types aren't supported

            Returns:
                [Tuple like]: field type, Field(title, default value)
            """
            value_type = type(field_value)

            if self._fields_defaults and field_name in self._fields_defaults:
                # if there is a value_default for the field, use it
                value_default = self._fields_defaults.get(field_name)
            else:
                # if there is preferred value_default for the type, use it
                if self._types_defaults and value_type in self._types_defaults:
                    value_default = self._types_defaults.get(value_type)
                    # otherwise, use the value_default for the type
                elif value_type in self._SCHEMA_TYPES_DEFAULTS:
                    value_default = self._SCHEMA_TYPES_DEFAULTS.get(value_type)
                else:

                    message = f"""
                        Invalid type for field '{field_name}': '{value_type}'.
                        Supported types: '{self._SCHEMA_TYPES_DEFAULTS.keys()}'
                    """
                    raise ValueError(message)

            # keep the title the same as the field name since the title is
            # the value used when generating the Json schema from the record,
            # which is also used to generate the Avro schema.
            #
            # If this is not done, the title will be different
            # from the field name when the Avro serializer is producing
            # the records, because the title uses 'snake case', therefore,
            # there will be no matching between the fields
            # which will take the serializer to assume the default values only
            if self._optional_fields and field_name in self._optional_fields:
                value_type = Optional[value_type]

            return value_type, Field(title=field_name, default=value_default)

        if self._types_defaults and self._types_defaults.get(float):
            self._types_defaults[Decimal] = self._types_defaults.get(float)

        template = {name: field_definition(
            name, value) for name, value in record.items()}
        return create_model(self._model_name, **template, __base__=self._base)
