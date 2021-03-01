"""
 Copyright 2021 ABSA Group Limited

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

from itertools import product
from typing import Any, Dict, List

import pydantic


def process_properties(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Processes a Pydantic generated schema to a confluent compliant schema

    :param schema: a valid pydantic schema
    :type schema: Dict[str, Any]
    :return: schema with `fields` and `names` keys
    :rtype: Dict[str, Any]
    """
    fields: List[Dict[str, Any]] = []
    for value in schema['properties'].values():
        try:
            value['name'] = value.pop('title')
            if value['type'] == 'number':
                value['type'] = 'double'
            if value['type'] == 'integer':
                value['type'] = 'int'
        finally:
            fields.append(value)
    schema['fields'] = fields
    return schema


def _find_optionals(model: pydantic.BaseModel) -> List[str]:
    """Helper function to parse pydantic ModelField to find optional
    types

    Args:
        model (pydantic.BaseModel): A Pydantic Model

    Returns:
        List[str]: Optional parameters as a list of strings
    """
    optionals = []
    for k, v in model.__fields__.items():
        type_ = str(v).split(' ')[1].replace('type=', '')
        if 'Optional' in type_:
            optionals.append(k)
    return optionals


def update_optional_schema(
        schema: Dict[str, Any],
        model: pydantic.BaseModel) -> Dict[str, Any]:
    _schema = schema.copy()
    optionals = _find_optionals(model)

    for field, optional_field in product(_schema['fields'], optionals):
        if field['name'] == optional_field:
            field.update({'type': ['null', field['type']]})
    return _schema
