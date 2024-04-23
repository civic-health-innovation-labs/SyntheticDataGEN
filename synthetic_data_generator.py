import csv
import json
import os
import secrets
import string
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Optional

# NOTE: no external dependencies are required

# ============ INPUTS ===============
# Path to the CSV, which defines the structure of the database, the outcome of:
# >>>
# SELECT
#     TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
#     ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
#     CHARACTER_MAXIMUM_LENGTH
# FROM INFORMATION_SCHEMA.COLUMNS;
# <<<
SQL_STRUCTURE_PATH: Path = Path(
    os.environ.get("SYNTHGEN_SQL_STRUCTURE_PATH", "inputs/SQL_SERVER_STRUCTURE.csv")
)
# Path to the JSON file that contains the number of rows per each table
#   following the logic: { "FULL_TABLE_NAME": NUMBER, ... }
#   It is a transformed outcome of the query:
# >>>
# SELECT
#     s.name AS SCHEMA_NAME, t.name AS TABLE_NAME, p.rows AS NR_ROWS
# FROM sys.tables t, sys.partitions p, sys.schemas s
# WHERE t.object_id = p.object_id and s.schema_id = t.schema_id;
# <<<
NUMBER_ROWS_PER_TABLE_PATH: Path = Path(
    os.environ.get(
        "SYNTHGEN_NUMBER_ROWS_PER_TABLE_PATH", "inputs/NUMBER_OF_ROWS_PER_TABLE.json"
    )
)
# Maximal number of rows generated to each table:
MAX_ROWS_PER_TABLE: int = int(os.environ.get("SYNTHGEN_MAX_ROWS_PER_TABLE", 1000))
# If True, CREATE statements are included in outputs (at the beginning):
GENERATE_CREATE_STATEMENTS: bool = bool(
    os.environ.get("SYNTHGEN_GENERATE_CREATE_STATEMENTS", True)
)
# If True, INSERT statements included in outputs:
GENERATE_INSERT_STATEMENTS: bool = bool(
    os.environ.get("SYNTHGEN_GENERATE_INSERT_STATEMENTS", True)
)

# Decide whether generated outputs are written into a file or printed
if OUTPUT_FILE := os.environ.get("SYNTHGEN_OUTPUT_FILE", False):
    OUTPUT_STREAM: callable = Path(OUTPUT_FILE).open("w").write
else:
    OUTPUT_STREAM: callable = print

# Maximal size of generated strings (e. g. varchar) in database:
MAX_STRING_SIZE: int = int(os.environ.get("SYNTHGEN_MAX_STRING_SIZE", 20))
# Maximal size of generated binary arrays (binary, varbinary) in DB:
MAX_BINARY_ARRAY_SIZE = int(os.environ.get("SYNTHGEN_MAX_BINARY_ARRAY_SIZE", 20))
# Maximum values for generated random integers
INTEGER_MAXIMUM = int(os.environ.get("SYNTHGEN_INTEGER_MAXIMUM", 36_000))
# Probability that the value is NULL for nullable columns
NULL_PROBABILITY = float(os.environ.get("SYNTHGEN_NULL_PROBABILITY_PERCENT", 20)) / 100
# Mapping to primary keys: TableName -> PrimaryKeys
PRIMARY_KEYS_PATH: Path = Path(
    os.environ.get("SYNTHGEN_PRIMARY_KEYS_PATH", "inputs/PRIMARY_KEYS.json")
)
# -----------------------------------

# =========== OPTIONS ===============
# Characters that occurs in generated strings (whole alphabet and digits)
alphabet: str = string.ascii_letters + string.digits
# -----------------------------------


# ======== FUNCTIONALITY ============
def parse_csv(
    path_to_csv: Path, column_names: list[str]
) -> tuple[int, dict[str, list[str | int]]]:
    """Transform input CSV file into dictionary with names of columns as
        a key and column values as an array (dictionary value).
    Args:
         path_to_csv (Path): Path to CSV file that is parsed.
         column_names (list[str]): List of column names.
    Returns:
        tuple[int, dict[str, list[str | int]]]: size of CSV (how many rows) and
            mapped CSV file as a dictionary. E. g.:
            (50, {"COLUMN_1": [1, "2nd row value", 3, ...], ...})
    """
    parsed_csv: dict[str, list[str | int]] = defaultdict(list)
    csv_size: int = 0
    with path_to_csv.open() as _csv_file:
        _csv_rows = csv.reader(_csv_file, delimiter=",")
        for _csv_row in _csv_rows:
            csv_size += 1
            for _position, _column in enumerate(column_names):
                parsed_csv[_column].append(_csv_row[_position])
    return csv_size, parsed_csv


def generate_table_definitions(
    number_of_rows_in_csv: int, parsed_table_def_csv: dict[str, list[str | int]]
) -> dict[str, dict[str, str | bool | Optional[int]]]:
    """Generates a dictionary that defines tables and columns inside tables.

    Args:
        number_of_rows_in_csv (int): Number of rows included in CSV.
        parsed_table_def_csv (dict[str, list[str | int]]): Definition of
            tables as interpreted CSV (see the return value of parse_csv).

    Returns:
        dict[str, dict[str, str | bool | Optional[int]]]: Definition of tables
            following the logic
            {
                "table_name": {
                    "column_1": {
                            "data_type": "varchar",
                            "is_nullable": True,
                            "max_length": 60
                    }, ...
                }, ...
            }
    """
    _table_defs: dict = defaultdict(dict)

    for _row_pos in range(number_of_rows_in_csv):
        # Reconstruct the full table name as TABLE_SCHEMA.TABLE_NAME
        _full_tbl_name = (
            f"{parsed_table_def_csv['TABLE_SCHEMA'][_row_pos]}."
            f"{parsed_table_def_csv['TABLE_NAME'][_row_pos]}"
        )

        # Treat maximum length (if NULL set to None, otherwise integer)
        _max_len = parsed_table_def_csv["CHARACTER_MAXIMUM_LENGTH"][_row_pos]
        if _max_len == "NULL":
            _max_len = None
        else:
            _max_len = int(_max_len)

        # Construct definition of a single column
        _column_definition = {
            parsed_table_def_csv["COLUMN_NAME"][_row_pos]: {
                "data_type": parsed_table_def_csv["DATA_TYPE"][_row_pos],
                "is_nullable": parsed_table_def_csv["IS_NULLABLE"][_row_pos] == "YES",
                "max_length": _max_len,  # Maximal length of varchar-like cols
            }
        }
        # Add a column into a table definition
        _table_defs[_full_tbl_name] |= _column_definition

    return _table_defs


def pad_string_by_apostrophes(input_string: str) -> str:
    """Add apostrophe symbols around string.
    Note:
        This function is added to make code more readable.
    Args:
        input_string (str): input string that is extended.
    Returns:
        str: String with apostrophes around
    """
    return f"'{input_string}'"


def random_value(
    data_type: str,
    str_size: int,
    bin_size: int,
    int_max: int,
    null_probability: Optional[float],
) -> Any:
    """Generates random sequence for given type.
    Note:
        Uses only types that occur in MS SQL database -> not universal.
        I. e. if you need additional data types, add them here.
    Args:
        data_type (str): what T-SQL data tape is input.
        str_size (int): default text size to be generated.
        bin_size (int): size for binary arrays.
        int_max (int): maximum for integers.
        null_probability (float): probability for NULL value in nullable
            columns. If None, column is not nullable.
    Raises:
        NotImplementedError: in the case there is no match to data type
    Returns:
        Any: random value for given type
    """
    if null_probability:
        # Treats null value
        if secrets.SystemRandom().uniform(0, 1) <= null_probability:
            return "NULL"

    match data_type:
        case "bit":
            return int(secrets.randbits(1))
        case "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext":
            return pad_string_by_apostrophes(
                "".join(secrets.choice(alphabet) for _ in range(str_size))
            )
        case "bigint" | "numeric" | "decimal" | "int":
            return secrets.randbelow(int_max)
        case "smallint":
            return secrets.randbelow(min(32767, int_max))
        case "tinyint":
            return secrets.randbelow(min(256, int_max))
        case "float":
            return secrets.randbelow(min(256, int_max)) / 50
        case "varbinary" | "binary":
            return f"CAST({secrets.randbits(bin_size)} AS BINARY({bin_size}))"
        case "date":
            return pad_string_by_apostrophes(
                f"{1970 + secrets.randbelow(60)}-"
                f"{1 + secrets.randbelow(12):02}-"
                f"{1 + secrets.randbelow(27):02}"
            )
        case "datetime" | "datetime2":
            return pad_string_by_apostrophes(
                f"{1970 + secrets.randbelow(60)}-"
                f"{1 + secrets.randbelow(12):02}-"
                f"{1 + secrets.randbelow(27):02} "
                f"{secrets.randbelow(24):02}:"
                f"{secrets.randbelow(60):02}:"
                f"{secrets.randbelow(60):02}"
            )
        case "time":
            return pad_string_by_apostrophes(
                f"{secrets.randbelow(24):02}:"
                f"{secrets.randbelow(60):02}:"
                f"{secrets.randbelow(60):02}"
            )
        case "timestamp":
            # This is a binary(8) equivalent
            return "DEFAULT"
        case "uniqueidentifier":
            return (
                "CONVERT(uniqueidentifier, "
                f"{pad_string_by_apostrophes(str(uuid.uuid4()))})"
            )

    raise NotImplementedError(f"unsupported data type {data_type}")


def create_statement(
    table_name: str,
    table_def: dict[str, dict[str, str | bool | Optional[int]]],
    primary_keys: Optional[list[str]] = None,
) -> str:
    """Generate CREATE TABLE statement for table
    Args:
        table_name (str): name of the table.
        table_def (dict[str, dict[str, str | bool | Optional[int]]]):
            definition of the table (see return val of
            generate_table_definitions function).
        primary_keys (Optional[list[str]]): List of columns that are primary
            keys (or None if none of them is).
    Return:
        str: Create statement for table
    """
    _column_definitions: list[str] = []
    for _column in table_def.keys():
        # Get the maximal length for varchar-like columns
        _max_length: Optional[int] | str = table_def[_column]["max_length"]
        if _max_length is not None and _max_length < 0:
            # Special treatment when the value is -1 -> the size is unlimited
            _max_length = "max"

        # Perform a special treatment for some data types
        match _data_type_def := table_def[_column]["data_type"]:
            case "char" | "varchar" | "nchar" | "nvarchar":
                _data_type_def += f"({_max_length})"
            case "varbinary" | "binary":
                _data_type_def += f"({_max_length})"
            case "bit":
                _data_type_def = "BIT"

        # Add NOT NULL to column definition if required
        _not_null = ""
        if not table_def[_column]["is_nullable"]:
            _not_null += "NOT NULL"
        # Defines one column
        _column_definitions.append(f"[{_column}] {_data_type_def} {_not_null}")

    # Definition of primary key
    _primary_key = ""
    if primary_keys:
        # Format the primary key name
        _primary_key_name = f"PK_{table_name.replace('.', '_')}"
        _primary_key = (
            f", CONSTRAINT [{_primary_key_name}] PRIMARY KEY ("
            + ",".join(primary_keys)
            + ")"
        )

    return (
        f"CREATE TABLE {table_name} ("
        + ",".join(_column_definitions)
        + _primary_key
        + ")"
    )


def insert_statement(
    table_name: str,
    table_def: dict[str, dict[str, str | bool | Optional[int]]],
    str_max_size: int,
    bin_max_size: int,
    int_max: int,
) -> tuple[dict[str, Any], str]:
    """Generate INSERT statement for given table.
    Args:
        table_name (str): name of the table.
        table_def (dict[str,dict[str, dict[str, str | bool | Optional[int]]]]):
            definition of the table (see return val of
            generate_table_definitions function).
        str_max_size (int): maximum size for strings (like varchar).
        bin_max_size (int): maximum size for bin arrays (like varbinary).
        int_max (int): maximum for integers.
    Returns:
         tuple[dict[str, Any], str]: A dictionary with column values
         and INSERT statement for given table
    """
    _values: dict[str, str] = {}
    for _col_name, _col_def in table_def.items():
        # Find the maximal size for varchar-like columns as minimum of
        #  their actual size and maximal size given as input
        _str_max_size = str_max_size
        _bin_max_size = bin_max_size
        if _col_def["max_length"] is not None and _col_def["max_length"] >= 0:
            # In case length < 0 => there is no limit in DB
            _str_max_size = min(str_max_size, _col_def["max_length"])
            _bin_max_size = min(bin_max_size, _col_def["max_length"])

        # Generates and append new entry
        _values[_col_name] = str(
            random_value(
                _col_def["data_type"],
                _str_max_size,
                _bin_max_size,
                int_max,
                NULL_PROBABILITY if _col_def["is_nullable"] else None,
            )
        )
    return (_values, f"INSERT INTO {table_name} VALUES({','.join(_values.values())})")


def stream_create_statements(
    table_definitions: dict,
    output_stream: Callable[[str], None],
    primary_keys: dict[str, Optional[list[str]]],
) -> None:
    """Stream CREATE statements for each table.

    Args:
        table_definitions (dict): definition of the table (see return val of
            generate_table_definitions function).
        output_stream (Callable[[str], None]): Function for data steaming,
            typically just print.
        primary_keys (dict[str, Optional[list[str]]]): Map of primary keys
            following the logic TableName -> [Column1, Column2, ...]
    """
    for _table_name, _table_def in table_definitions.items():
        # Main loop for generation of CREATE statements for tables
        _primary_keys = None
        if _table_name in primary_keys.keys():
            _primary_keys = primary_keys[_table_name]
        output_stream(create_statement(_table_name, _table_def, _primary_keys) + ";\n")


def stream_insert_statements(
    number_of_rows_per_table: dict,
    output_stream: Callable[[str], None],
    max_string_size: int,
    max_binary_array_size: int,
    integer_maximum: int,
    max_rows_per_table: Optional[int],
    primary_keys: dict[str, Optional[list[str]]],
) -> None:
    """Stream INSERT statements.

    Args:
        number_of_rows_per_table (dict): Mapping following logic the logic
            TableName -> NumberOfRows
        output_stream (Callable[[str], None]): Function for data steaming,
            typically just print.
        max_string_size (int): maximum size for strings (like varchar).
        max_binary_array_size (int): maximum size for bin arrays (like
            varbinary).
        integer_maximum (int): maximum for integers.
        max_rows_per_table (Optional[int]): Maximal number of rows per table,
            if None, does not apply
        primary_keys (dict[str, Optional[list[str]]]): Map of primary keys
            following the logic TableName -> [Column1, Column2, ...]
    """
    for _table_name, _table_def in _table_definitions.items():
        # To guarantee uniqueness of Primary Keys:
        _pk_set = set()

        _rows_per_table: int = number_of_rows_per_table[_table_name]
        if max_rows_per_table is not None:
            _rows_per_table = min(
                max_rows_per_table, number_of_rows_per_table[_table_name]
            )
        for pos in range(_rows_per_table):
            _col_values, _insert_stmt = insert_statement(
                _table_name,
                _table_def,
                max_string_size,
                max_binary_array_size,
                integer_maximum,
            )

            # Check if the Primary Key is unique, if not, repeat the process
            if _table_name in primary_keys.keys():
                _value: list = []
                if primary_keys[_table_name] is None:
                    continue
                for _pk_col in primary_keys[_table_name]:
                    _value.append(_col_values[_pk_col])
                if tuple(_value) in _pk_set:
                    pos += 1
                    continue
                else:
                    _pk_set.add(tuple(_value))

            output_stream(_insert_stmt + ";\n")


# -----------------------------------


# ==== ACTUAL "STAND-ALONE" PART ====
if __name__ == "__main__":
    # Load primary keys
    _primary_keys = json.load(PRIMARY_KEYS_PATH.open())

    # === GENERATE CREATE STATEMENTS ===
    _table_definitions: dict = generate_table_definitions(
        *parse_csv(
            SQL_STRUCTURE_PATH,
            [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "ORDINAL_POSITION",
                "COLUMN_DEFAULT",
                "IS_NULLABLE",
                "DATA_TYPE",
                "CHARACTER_MAXIMUM_LENGTH",
            ],
        )
    )
    if GENERATE_CREATE_STATEMENTS:
        stream_create_statements(_table_definitions, OUTPUT_STREAM, _primary_keys)
    # ----------------------------------

    # === GENERATE INSERT STATEMENTS ===
    if GENERATE_INSERT_STATEMENTS:
        _number_of_rows_per_table: dict = json.load(NUMBER_ROWS_PER_TABLE_PATH.open())
        stream_insert_statements(
            _number_of_rows_per_table,
            OUTPUT_STREAM,
            MAX_STRING_SIZE,
            MAX_BINARY_ARRAY_SIZE,
            INTEGER_MAXIMUM,
            MAX_ROWS_PER_TABLE,
            _primary_keys,
        )
# ----------------------------------
