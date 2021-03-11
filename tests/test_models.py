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
from unittest.mock import MagicMock, ANY

import pandas as pd
from pandas._testing import assert_frame_equal
import pytest

import py2k.models
from py2k.models import KafkaModel, DynamicKafkaModel


def test_dynamic_model_creates_pandas_model_creator(model_creator_class):
    model_name = 'TestModel'
    fields_defaults = {'field1': 10, 'field2': "aaa"}
    types_defaults = {int: 12}
    optional_fields = ['field4']

    params = ANY, model_name, fields_defaults, types_defaults, optional_fields
    DynamicKafkaModel(*params)

    params_to_call = *params, KafkaModel
    model_creator_class.assert_called_with(*params_to_call)


def test_dynamic_model_creates_pandas_model_creator_from_dataframe(model_creator_class):
    df = pd.DataFrame({'a': [1, 2], 'b': ["bla", "alb"]})

    params = df, ANY, ANY, ANY, ANY
    DynamicKafkaModel(*params)

    params_to_call = *params, KafkaModel
    called_df, *_ = called_args(model_creator_class, len(params_to_call))
    assert_frame_equal(df, called_df)


@pytest.fixture
def model_creator():
    return MagicMock()


@pytest.fixture(autouse=True)
def model_creator_class(monkeypatch, model_creator):
    model_creator_class = MagicMock()
    model_creator_class.return_value = model_creator
    monkeypatch.setattr(py2k.models,
                        'PandasModelCreator', model_creator_class)
    return model_creator_class


def called_args(mock, n_of_expected):
    all_args = mock.call_args
    args = None if all_args is None else list(all_args[0])
    args += [None] * (n_of_expected - len(args))
    return args
