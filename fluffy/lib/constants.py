SQL_COMPOSITE_TYPE_COLUMNS = """
SELECT attname,
       p.typname,
       'USER-DEFINED' as data_type,
       'USER-DEFINED' as udt_name,
       %(column_name)s as column_name
FROM pg_attribute
JOIN pg_type p
ON p.typelem = atttypid
WHERE attrelid = (SELECT typrelid from pg_type WHERE typname in (%(data_type)s));
"""

SQL_GET_ENUMS = """
SELECT e.enumlabel
  FROM pg_enum e
  JOIN pg_type t ON e.enumtypid = t.oid
  WHERE t.typname = %(enum_name)s
"""

SQL_GET_COLUMNS = """
SELECT column_name,
       udt_name,
       data_type,
       is_nullable,
       column_default
FROM information_schema.columns
WHERE table_name = %(table)s
AND column_name not in %(primary_keys)s
"""
