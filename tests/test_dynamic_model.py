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

import json
from copy import deepcopy
from datetime import date
from decimal import Decimal
from typing import Optional

import pandas as pd
import pytest
from pydantic import BaseModel

from py2k.creators import PandasModelCreator
from py2k.record import PandasToRecordsTransformer, KafkaRecord


class _TestData(BaseModel):
    name: str
    id: int
    value: float
    decimal_val: Decimal
    bool_val: bool

    def __str__(self):
        return f"Name = ${self.name}, " \
               f"Id = ${self.id}, " \
               f"Value = ${self.value}, " \
               f"Decimal Val = ${self.decimal_val}," \
               f" bool_val = ${self.bool_val}"

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


class _TestDataWithOptional(BaseModel):
    name: Optional[str]
    id: int

    def __str__(self):
        return f"Name = ${self.name}, Id = ${self.id}"

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


def test_return_empty_if_df_empty(test_df):
    with pytest.warns(UserWarning) as record:
        empty_df = test_df.head(0)

        model = PandasToRecordsTransformer(test_df, 'TestModel')
        created_df = model.from_pandas(empty_df)
        assert len(created_df) == 0

    expected_message = "Unable to create kafka model from an empty dataframe."
    assert len(record) == 1
    assert record[0].message.args[0] == expected_message


def test_dynamically_convert_from_pandas(test_data, test_df):
    model = PandasToRecordsTransformer(test_df, 'TestModel')
    records = model.from_pandas(test_df)
    _assert_records(records, test_data)


def test_convert_from_constructed_dataframe_by_default(test_data, test_df):
    model = PandasToRecordsTransformer(test_df, 'TestModel')
    records = model.from_pandas()
    _assert_records(records, test_data)


def test_fields_names_and_titles_are_the_same(test_df):
    model = PandasToRecordsTransformer(test_df, 'TestModel')
    records = model.from_pandas(test_df)

    for name, definition in records[0].__fields__.items():
        assert name == definition.name


def test_use_default_defaults_if_none_informed(test_df):
    model = PandasToRecordsTransformer(test_df, 'TestModel')
    records = model.from_pandas(test_df)

    # expected defaults by field
    expected = {
        field: PandasModelCreator._SCHEMA_TYPES_DEFAULTS.get(type(value))
        for field, value in records[0].dict().items()}

    _assert_schema_defaults(records[0], expected)


def test_all_defaults_from_field_name(test_df):
    expected = {"name": "default name", "id": 8,
                "value": 8.8, "decimal_val": 8.8, "bool_val": True}

    model = PandasToRecordsTransformer(test_df, 'TestModel',
                                       fields_defaults=expected)
    records = model.from_pandas(test_df)

    _assert_schema_defaults(records[0], expected)


def test_some_defaults_from_field_name(test_df):
    local_defaults = {"name": "default name", "value": 8.8,
                      "decimal_val": Decimal(12), "bool_val": True}

    model = PandasToRecordsTransformer(test_df, 'TestModel',
                                       fields_defaults=local_defaults)
    records = model.from_pandas(test_df)

    expected = {**local_defaults,
                "id": PandasModelCreator._SCHEMA_TYPES_DEFAULTS.get(int)}

    _assert_schema_defaults(records[0], expected)


def test_all_defaults_from_field_type(test_df):
    expected = {str: "default name", int: 8, float: 8.8, bool: True}

    model = PandasToRecordsTransformer(test_df, 'TestModel',
                                       types_defaults=expected)
    records = model.from_pandas(test_df)

    _assert_schema_defaults(records[0], expected, by_name=False)


def test_some_defaults_from_field_type(test_df):
    local_defaults = {int: 8, float: 8.8, bool: True}

    model = PandasToRecordsTransformer(test_df, 'TestModel',
                                       types_defaults=local_defaults)
    records = model.from_pandas(test_df)

    expected = {**local_defaults,
                str: PandasModelCreator._SCHEMA_TYPES_DEFAULTS.get(str)}

    _assert_schema_defaults(records[0], expected, by_name=False)


def test_optional_fields_specified_by_param(test_df_with_nones):
    model = PandasToRecordsTransformer(test_df_with_nones, 'TestModel',
                                       optional_fields=['name'])
    records = model.from_pandas(test_df_with_nones)

    expected = {"name": Optional[str], "id": int}
    _assert_schema_types(records[0], expected)


_raw_types_test_cases = [
    (10, int),
    ("aa", str),
    (12.4, float),
    (True, bool),
    (Decimal(10.), float),
    (date(2020, 1, 10), date),
    (pd.Timestamp('2020-01-01T12'), pd.Timestamp),
]
_optional_types_test_cases = [(value, Optional[_type])
                              for (value, _type) in _raw_types_test_cases]


@pytest.mark.parametrize("value,_type", _raw_types_test_cases)
def test_supported_types(value, _type):
    df = pd.DataFrame({'column_1': [value]})

    model = PandasToRecordsTransformer(df, 'TestModel')
    records = model.from_pandas(df)

    expected = {'column_1': _type}

    _assert_schema_types(records[0], expected)


@pytest.mark.parametrize("value,_type", _optional_types_test_cases)
def test_supported_optional_types(value, _type):
    df = pd.DataFrame({'column_1': [value]})

    model = PandasToRecordsTransformer(df, 'TestModel',
                                       optional_fields=['column_1'])
    records = model.from_pandas(df)

    expected = {'column_1': _type}

    _assert_schema_types(records[0], expected)


def test_specification_of_key_fields():
    df = pd.DataFrame({'column_1': [1], 'key': ['key_val']})

    model = PandasToRecordsTransformer(df, 'TestModel', key_fields={'key'})
    record = model.from_pandas(df)[0]
    assert record.key_fields == {'key'}


def _assert_records(records, test_data):
    # makes sure all records were converted
    assert len(records) == len(test_data)

    reconstructions = []
    for record in records:
        # makes sure the extracted models are instances of KafkaModel
        assert issubclass(record.__class__, KafkaRecord)

        reconstructions.append(_TestData(**record.dict()))

    # makes sure the data is the same
    assert set(test_data) == set(reconstructions)


def _assert_schema_defaults(sample_record, expected, by_name=True):
    def json_to_python_type(type_name):
        return {
            "string": str,
            "int": int,
            "double": float,
            "boolean": bool
        }.get(type_name)

    def python_val_to_avro(val):
        return {
            False: 'false',
            True: 'true'
        }.get(val, val)

    # actual records after transformed
    actual = json.loads(sample_record.schema_json())

    # asserts the default defaults were used
    for field in actual["fields"]:
        actual_default = field.get("default")
        if by_name:
            expected_default = expected.get(field.get("name").lower())
        else:
            expected_default = expected.get(
                json_to_python_type(field.get("type")))

        expected_default = python_val_to_avro(expected_default)
        assert actual_default == expected_default


def _assert_schema_types(sample_record, expected):
    def json_field(_type, **rest):
        type_dict = {'type': _type}
        return {**type_dict, **rest}

    def field_to_optional(_json_field):
        optional_field = deepcopy(_json_field)
        optional_type = tuple(['null', optional_field.get('type')])
        optional_field['type'] = optional_type
        return optional_field

    def python_to_json(_type):
        raw_types = {
            int: json_field('int'),
            str: json_field('string'),
            float: json_field('double'),
            bool: json_field('boolean'),
            date: json_field('string', format='date'),
            pd.Timestamp: json_field('string', format='date-time')
        }

        optional_types = {Optional[raw_type]: field_to_optional(
            _json_field) for raw_type, _json_field in raw_types.items()}
        types_dict = {**raw_types, **optional_types}
        return types_dict.get(_type)

    # actual records after transformed
    actual = json.loads(sample_record.schema_json())

    for field in actual["fields"]:
        # deal with that lists cannot be hashed in sets
        field = {k: tuple(v) if type(v) == list else v for k,
                 v in field.items()}
        field_as_set = set(field.items())

        expected_type = expected.get(field.get("name").lower())
        expected_json = python_to_json(expected_type)
        expected_json_as_set = set(expected_json.items())
        assert expected_json_as_set.issubset(field_as_set)


@pytest.fixture
def test_data():
    return [
        _TestData(name="a", id=1, value=2.5,
                  decimal_val=Decimal(12), bool_val=True),
        _TestData(name="b", id=2, value=3.4,
                  decimal_val=Decimal(22), bool_val=True),
        _TestData(name="c", id=3, value=7.1,
                  decimal_val=Decimal(32), bool_val=False),
    ]


@pytest.fixture
def test_df(test_data):
    return pd.DataFrame([d.__dict__ for d in test_data])


@pytest.fixture
def test_data_with_nones():
    return [
        _TestDataWithOptional(name="a", id=1),
        _TestDataWithOptional(name=None, id=2),
        _TestDataWithOptional(name="c", id=3),
    ]


@pytest.fixture
def test_df_with_nones(test_data_with_nones):
    return pd.DataFrame([d.__dict__ for d in test_data_with_nones])
