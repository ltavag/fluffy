import cerberus
from cerberus import Validator
import psycopg2
import datetime
from psycopg2.extras import (
        NumericRange,
        DateRange,
        DateTimeRange,
        Json
)


class PostgresValidator(Validator):
    types_mapping = Validator.types_mapping.copy()
    types_mapping.update({
        'varchar': cerberus.TypeDefinition('varchar', (str,), ()),
        'int4': cerberus.TypeDefinition('int4', (int,), ()),
        'int': cerberus.TypeDefinition('int', (int,), ()),
        'text': cerberus.TypeDefinition('text', (str,), ()),
        'citext': cerberus.TypeDefinition('citext', (str,), ()),
        'bool': cerberus.TypeDefinition('bool', (bool,), ()),
        'json': cerberus.TypeDefinition('json', (Json,), ()),
        'jsonb': cerberus.TypeDefinition('jsonb', (Json,), ()),
    })
    types_mapping.pop('date')
    types_mapping.pop('datetime')

    @staticmethod
    def all_types(self):
        simple_types = list(self.types_mapping.keys())
        complex_types = [x.replace('_validate_type_', '')
                         for x in dir(self)
                         if x.startswith('_validate_type_')]
        return simple_types + complex_types

    @staticmethod
    def coerce_types(self):
        return [x.replace('_normalize_coerce_', '')
                for x in dir(self)
                if x.startswith('_normalize_coerce_')]

    def _validate_type_int4range(self, value):
        if isinstance(value, NumericRange):
            return True

    def _normalize_coerce_int4range(self, value):
        assert all(isinstance(x, int) for x in value)
        assert len(value) == 2

        return NumericRange(*value)

    def _validate_type_date(self, value):
        if isinstance(value, datetime.date):
            return True

    def _normalize_coerce_date(self, value):
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()

    def _validate_type_datetime(self, value):
        if isinstance(value, datetime.datetime):
            return True

    def _normalize_coerce_datetime(self, value):
        return datetime.datetime.strptime(value, '%Y-%m-%d')

    def _validate_type_daterange(self, value):
        if isinstance(value, DateRange):
            return True

    def _normalize_coerce_daterange(self, value):
        datetimes = map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date, value)
        return DateRange(*datetimes)

    def _validate_type_tsrange(self, value):
        if isinstance(value, DateTimeRange):
            return True

    def _normalize_coerce_tsrange(self, value):
        datetimes = map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), value)
        return DateTimeRange(*datetimes)

    def _normalize_coerce_json(self, value):
        return Json(value)

    def _normalize_coerce_jsonb(self, value):
        return Json(value)
