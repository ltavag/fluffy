from .postgres_validator import PostgresValidator
import psycopg2
from psycopg2 import sql


class TableSchema(dict):

    def __init__(self, **data):
        v = self._validator(self._schema, purge_unknown=True)
        validated = v.validated(data)
        if not validated:
            raise Exception(v.errors)

        self.update(v.normalized(data))
        self.coalesce()

    def coalesce(self):
        for k in self:
            coalesce_fn = '_coalesce_{table}_{field}'.format(
                    table=self._table,
                    field=k
            )

            if hasattr(self, coalesce_fn):
                self[k] = getattr(self, coalesce_fn)(self[k])


    def insert_sql(self):
        query = """
INSERT INTO {table}
({fields})
VALUES
({values});"""

        fields = ', '.join((sql.Identifier(x).as_string(self._cursor)
                            for x in self.keys()))
        values = ', '.join(("%({})s".format(x) for x in self.keys()))
        query = query.format(fields=fields, values=values, table=self._table)
        return self._cursor.mogrify(query, self).decode('utf-8')

    def update_sql(self, primary_key_filters):
        query = """
UPDATE
{table}
SET {field_updates}
WHERE {primary_keys_filters};"""

        field_updates = '\n\t,'.join(['{}={}'.format(sql.Identifier(x).as_string(self._cursor),
                                                     '%({})s'.format(x))
                                     for x in self])

        pk_filters = '\nAND '.join(['{}={}'.format(sql.Identifier(x).as_string(self._cursor),
                                                   sql.Literal(primary_key_filters[x]).as_string(self._cursor))
                                   for x in self._primary_keys])

        query = query.format(field_updates=field_updates,
                             primary_keys_filters=pk_filters,
                             table=self._table)

        return self._cursor.mogrify(query, self).decode('utf-8')
