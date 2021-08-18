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
from collections import Iterator
from unittest.mock import MagicMock, ANY
import datetime as dt

import pandas as pd
from pandas._testing import assert_frame_equal
import pytest

import py2k.record
from py2k.record import KafkaRecord, PandasToRecordsTransformer


def test_dynamic_model_creates_pandas_model_creator(model_creator_class):
    model_name = 'TestModel'
    fields_defaults = {'field1': 10, 'field2': "aaa"}
    types_defaults = {int: 12}
    optional_fields = ['field4']

    params = ANY, model_name, fields_defaults, types_defaults, optional_fields
    PandasToRecordsTransformer(*params)

    params_to_call = *params, ANY
    model_creator_class.assert_called_with(*params_to_call)


def test_dynamic_model_creates_creator_from_dataframe(model_creator_class):
    df = pd.DataFrame({'a': [1, 2], 'b': ["bla", "alb"]})

    params = df, ANY, ANY, ANY, ANY
    PandasToRecordsTransformer(*params)

    params_to_call = *params, KafkaRecord
    called_df, *_ = called_args(model_creator_class, len(params_to_call))
    assert_frame_equal(df, called_df)


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_user_defines_model_without_key(method_name):
    class MyRecord(KafkaRecord):
        value_1: str
        value_2: int

    df = pd.DataFrame({
        'value_1': ['a', 'b'],
        'value_2': [1, 2]
    })

    from_pandas_method = getattr(MyRecord, method_name)
    record = list(from_pandas_method(df))[0]
    assert record.key_fields == {}


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_user_defines_model_with_one_key(pandas_data, method_name):
    class MyRecord(KafkaRecord):
        value_1: str
        key_field: str
        __key_fields__ = {'key_field'}

    from_pandas_method = getattr(MyRecord, method_name)
    record = list(from_pandas_method(pandas_data))[0]
    assert record.key_fields == {'key_field'}


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_dynamic_defines_key_fields(pandas_data, method_name):
    model = PandasToRecordsTransformer(pandas_data, "MyRecord",
                                       key_fields={'key_field'})

    from_pandas_method = getattr(model, method_name)
    record = list(from_pandas_method(pandas_data))[0]
    assert record.key_fields == {'key_field'}


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_dynamic_defines_key_included(pandas_data, method_name):
    model = PandasToRecordsTransformer(pandas_data, "MyRecord",
                                       key_fields={'key_field'},
                                       include_key=True)

    from_pandas_method = getattr(model, method_name)
    record = list(from_pandas_method(pandas_data))[0]
    assert record.include_key


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_dynamic_key_included_false_as_default(pandas_data, method_name):
    model = PandasToRecordsTransformer(pandas_data, "MyRecord",
                                       key_fields={'key_field'})

    from_pandas_method = getattr(model, method_name)
    record = list(from_pandas_method(pandas_data))[0]
    assert not record.include_key


@pytest.mark.parametrize(
    'method_name, expected_type',
    [('from_pandas', list),
     ('iter_from_pandas', Iterator)],
    ids=['from_pandas', 'iter_from_pandas']
)
def test_user_defined_correct_iter(pandas_data, method_name, expected_type):
    class MyRecord(KafkaRecord):
        value_1: str
        key_field: str
        __key_fields__ = {'key_field'}

    from_pandas_method = getattr(MyRecord, method_name)
    result = from_pandas_method(pandas_data)
    assert isinstance(result, expected_type)


@pytest.mark.parametrize(
    'method_name, expected_type',
    [('from_pandas', list),
     ('iter_from_pandas', Iterator)],
    ids=['from_pandas', 'iter_from_pandas']
)
def test_dynamic_defined_correct_iter(pandas_data, method_name, expected_type):
    model = PandasToRecordsTransformer(pandas_data, "MyRecord",
                                       key_fields={'key_field'})

    from_pandas_method = getattr(model, method_name)
    result = from_pandas_method(pandas_data)
    assert isinstance(result, expected_type)


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_dynamic_creates_from_original_by_default(pandas_data, method_name):
    model = PandasToRecordsTransformer(pandas_data, "MyRecord",
                                       key_fields={'key_field'})

    from_pandas_method = getattr(model, method_name)
    record = list(from_pandas_method())[0]
    assert not record.include_key


@pytest.mark.parametrize(
    'method_name',
    ['from_pandas', 'iter_from_pandas']
)
def test_empty_df_return_empty_iter(method_name):
    class MyRecord(KafkaRecord):
        value_1: str
        value_2: int

    df = pd.DataFrame({
        'value_1': [],
        'value_2': []
    })
    with pytest.warns(UserWarning) as warnings:
        from_pandas_method = getattr(MyRecord, method_name)
        records = list(from_pandas_method(df))

    assert records == []
    assert len(warnings) == 1
    assert warnings[0].message.args[0] == "Unable to create kafka " \
                                          "model from an empty dataframe."


@pytest.mark.parametrize(
    'value',
    ['bla', 12, -20., False, dt.date(2020, 1, 1),
     dt.datetime(2020, 1, 1, 0, 0, 0)],
    ids=['str', 'int', 'float', 'bool', 'date', 'datetime']
)
def test_dynamic_with_null_first_row(value):
    df = pd.DataFrame({'a': [None, value]})

    model = PandasToRecordsTransformer(df, "MyRecord", optional_fields=['a'])

    records = model.from_pandas()
    assert pd.isna(records[0].a)  # pd.isna(None) == true
    assert records[1].a == value


def test_one_column_all_nulls():
    df = pd.DataFrame({
        'non_empty_col': [1, 2],
        'empty_col': [None, None]
    })

    with pytest.raises(ValueError) as exception_info:
        model = PandasToRecordsTransformer(df, "MyRecord",
                                           optional_fields=['a'])
        model.from_pandas()

    assert "Invalid type for field 'empty_col'" in str(exception_info.value)


@pytest.fixture
def pandas_data():
    return pd.DataFrame({
        'value_1': ['a', 'b'],
        'key_field': [1, 2]
    })


@pytest.fixture
def model_creator():
    return MagicMock()


@pytest.fixture
def model_creator_class(monkeypatch, model_creator):
    model_creator_class = MagicMock()
    model_creator_class.return_value = model_creator
    monkeypatch.setattr(py2k.record,
                        'PandasModelCreator', model_creator_class)
    return model_creator_class


def called_args(mock, n_of_expected):
    all_args = mock.call_args
    args = None if all_args is None else list(all_args[0])
    args += [None] * (n_of_expected - len(args))
    return args
