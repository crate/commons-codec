# ruff: noqa: A005
# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# https://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
# Patched: from boto3.compat import collections_abc
from decimal import (
    Clamped,
    Context,
    Inexact,
    Overflow,
    Rounded,
    Underflow,
)

STRING = "S"
NUMBER = "N"
BINARY = "B"
STRING_SET = "SS"
NUMBER_SET = "NS"
BINARY_SET = "BS"
NULL = "NULL"
BOOLEAN = "BOOL"
MAP = "M"
LIST = "L"


DYNAMODB_CONTEXT = Context(
    Emin=-128,
    Emax=126,
    prec=38,
    traps=[Clamped, Overflow, Inexact, Rounded, Underflow],
)


BINARY_TYPES = (bytearray, bytes)


class Binary:
    """A class for representing Binary in dynamodb

    Especially for Python 2, use this class to explicitly specify
    binary data for item in DynamoDB. It is essentially a wrapper around
    binary. Unicode and Python 3 string types are not allowed.
    """

    def __init__(self, value):
        if not isinstance(value, BINARY_TYPES):
            types = ", ".join([str(t) for t in BINARY_TYPES])
            raise TypeError(f"Value must be of the following types: {types}")
        self.value = value

    def __eq__(self, other):
        if isinstance(other, Binary):
            return self.value == other.value
        return self.value == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f"Binary({self.value!r})"

    def __str__(self):
        return self.value

    def __bytes__(self):
        return self.value

    def __hash__(self):
        return hash(self.value)


class TypeDeserializer:
    """This class deserializes DynamoDB types to Python types."""

    def deserialize(self, value):
        """The method to deserialize the DynamoDB data types.

        :param value: A DynamoDB value to be deserialized to a pythonic value.
            Here are the various conversions:

            DynamoDB                                Python
            --------                                ------
            {'NULL': True}                          None
            {'BOOL': True/False}                    True/False
            {'N': str(value)}                       Decimal(str(value))
            {'S': string}                           string
            {'B': bytes}                            Binary(bytes)
            {'NS': [str(value)]}                    set([Decimal(str(value))])
            {'SS': [string]}                        set([string])
            {'BS': [bytes]}                         set([bytes])
            {'L': list}                             list
            {'M': dict}                             dict

        :returns: The pythonic value of the DynamoDB type.
        """

        if not value:
            raise TypeError("Value must be a nonempty dictionary whose key is a valid dynamodb type.")
        dynamodb_type = list(value.keys())[0]
        try:
            deserializer = getattr(self, f"_deserialize_{dynamodb_type}".lower())
        except AttributeError as ex:
            raise TypeError(f"Dynamodb type {dynamodb_type} is not supported") from ex
        return deserializer(value[dynamodb_type])

    def _deserialize_null(self, value):
        return None

    def _deserialize_bool(self, value):
        return value

    def _deserialize_n(self, value):
        return DYNAMODB_CONTEXT.create_decimal(value)

    def _deserialize_s(self, value):
        return value

    def _deserialize_b(self, value):
        return Binary(value)

    def _deserialize_ns(self, value):
        return set(map(self._deserialize_n, value))

    def _deserialize_ss(self, value):
        return set(map(self._deserialize_s, value))

    def _deserialize_bs(self, value):
        return set(map(self._deserialize_b, value))

    def _deserialize_l(self, value):
        return [self.deserialize(v) for v in value]

    def _deserialize_m(self, value):
        return {k: self.deserialize(v) for k, v in value.items()}
