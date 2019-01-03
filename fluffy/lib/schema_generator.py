from .constants import (
        SQL_COMPOSITE_TYPE_COLUMNS,
        SQL_GET_ENUMS,
        SQL_GET_COLUMNS
)


class SchemaGenerator:

    def __init__(self, validator, cursor, primary_keys):
        self.validator = validator
        self.cursor = cursor
        self.primary_keys = primary_keys

    def coalesce_data_type(self, x):
        """
        Return a data type from a typname as in a manner
        similar to the Postgres information schema tables.
        """
        if x.lstrip('_') in self.validator.all_types(self.validator):
            return x

        return 'USER-DEFINED'

    def get_coerce_type(self, x):
        """
        If a custom coercer is defined, use it here.
        """
        if x in self.validator.coerce_types(self.validator):
            return x

    def get_column_default(self, column_desc):
        """
        Function that retrieves the default value for a given field.

        :type cursor: class `psycopg2.connection.cursor()`
        :param cursor: An open cursor to the database for information schema queries.
        :type column_desc: `dict`
        :param column_desc: A dictionary that contains metadata about the data field.

        Note: This call is potentially unsafe as it uses Python's native string substitution
        for a parameterized query. This is because of how Postgres column defaults are stored.

        Note: This function can potentially also burn a sequence ID if called with a field
        that defaults to a sequence ( e.g. Almost any primary key )

        """
        if column_desc.get('column_default'):
            self.cursor.execute('SELECT {} AS default'.format(column_desc['column_default']))
            value = self.cursor.fetchone()
            return value['default']

        return None

    def create_schema_from_field(self,
                                 column_desc):
        """
        Function that recursively traverses field of each table.

        :type cursor: class `psycopg2.connection.cursor()`
        :param cursor: An open cursor to the database for information schema queries.
        :type column_desc: `dict`
        :param column_desc: A dictionary that contains metadata about the data field.
        :type schema: `dict`
        :param schema: Cerberus schema which is being built out through recursive \
        calls to the function.
        """
        schema = {}

        if any([column_desc['data_type'] not in ('ARRAY', 'USER-DEFINED'),
                column_desc['udt_name'] == 'citext']):
            """
            First, we deal with the simple Postgres types that map directly in to
            python builtin types. This code path has no recursive calls.
            """

            column_name = column_desc['column_name']
            column_data_type = column_desc['udt_name']
            required = column_desc['is_nullable'] == 'NO'

            schema = {
                    'type': column_data_type.lstrip('_'),
                    'required': required
            }

            if not required:
                schema['nullable'] = True
                schema['default'] = None

            default = self.get_column_default(column_desc)
            if default:
                schema['default'] = default
                schema['required'] = False

            coercer = self.get_coerce_type(column_data_type.lstrip('_'))
            if coercer:
                schema['coerce'] = coercer


        elif column_desc['data_type'] == 'USER-DEFINED':
            """
            Next, we deal with user defined postgres types. Currently we deal with the
            specific cases of:
                * User defined composite types.
                * User defined enum types.

            We first check to see if it is a composite type, if it's not we check to
            see if it is a user defined enum.

            Note: Only in the case of composite types do recursive calls get made.
            """
            self.cursor.execute(SQL_COMPOSITE_TYPE_COLUMNS,
                           {
                               "data_type": column_desc['udt_name'].lstrip('_'),
                               "column_name": column_desc['column_name']
                           })
            composite_rows = self.cursor.fetchall()
            if composite_rows:
                schema = {
                        'type': 'dict',
                        'schema': {}
                }

                for composite_row in composite_rows:
                    column_name = composite_row['attname']
                    coercer = self.get_coerce_type(composite_row['column_name'].lstrip('_'))
                    if coercer:
                        schema['coerce'] = coercer

                    schema['schema'][column_name] = self.create_schema_from_field(
                            {
                                'data_type': self.coalesce_data_type(composite_row['typname']),
                                'column_name': composite_row['attname'],
                                'udt_name': composite_row['typname'].lstrip('_'),
                                'is_nullable': 'YES'
                            })

            else:
                # Maybe its an enum
                self.cursor.execute(SQL_GET_ENUMS, {"enum_name": column_desc["udt_name"].lstrip('_')})
                enum_values = self.cursor.fetchall()

                values = [x['enumlabel'] for x in enum_values]

                column_name = column_desc['column_name']
                required = column_desc['is_nullable'] == 'NO'

                schema = {
                        'type': 'string',
                        'allowed': values,
                        'required': required
                }

                if not required:
                    schema['allowed'].append(None)
                    schema['nullable'] = True
                    schema['default'] = None

                default = self.get_column_default(column_desc)
                if default:
                    schema['default'] = default
                    schema['required'] = False

        elif column_desc['data_type'] == 'ARRAY':
            """
            Finally we deal with arrays of other types. We identify this column as type
            list, before recursively calling the function to resolve the type of items
            in the list.
            """
            column_name = column_desc['column_name']
            required = column_desc['is_nullable'] == 'NO'

            if column_name not in schema:
                schema = {
                        'type': 'list',
                        'required': required,
                        'schema': {}
                }

                if not required:
                    schema['default'] = []

            schema['schema'] = self.create_schema_from_field(
                    {
                        'data_type': self.coalesce_data_type(column_desc['udt_name']),
                        'column_name': column_desc['udt_name'].lstrip('_'),
                        'udt_name': column_desc.get('typname', column_desc['udt_name']),
                        'is_nullable': 'YES'
                    })

        return schema


    def create_schema_for_table(self, table_name):
        """
        Create cerberus schema given a cursor and a table name.

        :type cursor: class `psycopg2.connection.cursor()`
        :param cursor: An open cursor to the database for information schema queries.
        :type table_name: `str`
        :param table_name: The table name for which we are generating a cerberus schema.
        """
        self.cursor.execute(SQL_GET_COLUMNS, {
            'table': table_name,
            'primary_keys': self.primary_keys
        })

        voter_record_fields = self.cursor.fetchall()
        schema = {}
        for field in voter_record_fields:
            schema[field['column_name']] = self.create_schema_from_field(field)

        return schema
