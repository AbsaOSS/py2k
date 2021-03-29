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

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from py2k.creators import PandasModelCreator
from py2k.models import KafkaRecord


@pytest.mark.parametrize(
    'expected_type, value',
    [
        (str, 'str_value'),
        (int, 10),
        (float, -1.0),
        (bool, True),
        (Decimal, Decimal(43.2)),
        (date, date(1991, 3, 23)),
        (pd.Timestamp, pd.Timestamp(1991, 3, 23, 12)),
    ]
)
def test_creates_model_with_field_of_type(expected_type, value):
    df = pd.DataFrame({'field_name': [value]})

    model = PandasModelCreator(df, 'TestModel', base=KafkaRecord).create()
    fields = model.__fields__
    field = list(fields.values())[0]

    assert len(fields) == 1
    assert field.name == 'field_name'
    assert field.type_ == expected_type


def test_throw_exception_when_creating_model_from_empty_df():
    with pytest.raises(ValueError):
        empty_df = pd.DataFrame(data=[], columns=['c1', 'c2', 'c3'])
        PandasModelCreator(empty_df, 'TestModel', base=KafkaRecord).create()
