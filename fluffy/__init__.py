from .lib.schema_generator import SchemaGenerator
from .lib.valid_schema import TableSchema
from .lib.postgres_validator import PostgresValidator


def create_validator(cursor,
                     model_name,
                     table_name,
                     primary_keys=('id',),
                     custom_validator=PostgresValidator,
                     mixins=[]):

    s = SchemaGenerator(custom_validator, cursor, primary_keys)
    return type(model_name,
                (TableSchema, *mixins),
                {
                    '_schema': s.create_schema_for_table(table_name),
                    '_table': table_name,
                    '_cursor': cursor,
                    '_primary_keys': primary_keys,
                    '_validator': custom_validator
                })
