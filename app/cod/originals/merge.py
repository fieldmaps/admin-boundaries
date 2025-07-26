import logging

from psycopg.sql import SQL, Identifier

from .utils import get_cols

logger = logging.getLogger(__name__)

query_1 = """
    ALTER TABLE {table_out}
    ADD COLUMN IF NOT EXISTS {column} VARCHAR DEFAULT NULL;
"""

query_2 = """
    DELETE FROM {table_in1} WHERE ({col}) IN (
        SELECT {col} FROM {table_in2}
    );
"""

# query_2a = """
#     DROP TABLE IF EXISTS {table_out};
#     CREATE TABLE {table_out} AS
#     SELECT DISTINCT ON (a.geom)
#         {ids},
#         ST_Snap(a.geom, b.geom, 0.000001) as geom
#     FROM {table_in1} AS a, {table_in2} AS b;
# """

query_3 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT {ids} FROM {table_in1}
    UNION ALL
    SELECT {ids} FROM {table_in2};
"""

query_4 = """
    ALTER TABLE {table_in}
    RENAME TO {table_out};
"""

drop_tmp = """
    DROP TABLE IF EXISTS {table_in1};
    DROP TABLE IF EXISTS {table_in2};
"""


def main(conn, name, level, level_max, row):
    if level_max is not None:
        ids = get_cols(level_max, row)
        for col in ids:
            conn.execute(
                SQL(query_1).format(
                    column=Identifier(col),
                    table_out=Identifier(f"{name}_adm{level}"),
                ),
            )
        conn.execute(
            SQL(query_2).format(
                col=Identifier(f"ADM{level}_PCODE"),
                table_in1=Identifier(f"{name}_adm{level}"),
                table_in2=Identifier(f"{name}_adm{level_max}"),
            ),
        )
        # conn.execute(
        #     SQL(query_2a).format(
        #         ids=SQL(",").join(map(lambda x: Identifier("a", x), ids)),
        #         table_in1=Identifier(f"{name}_adm{level}_00"),
        #         table_in2=Identifier(f"{name}_adm{level_max}_00"),
        #         table_out=Identifier(f"{name}_adm{level}_01"),
        #     )
        # )
        conn.execute(
            SQL(query_3).format(
                ids=SQL(",").join(map(Identifier, ids + ["geom"])),
                table_in1=Identifier(f"{name}_adm{level}"),
                table_in2=Identifier(f"{name}_adm{level_max}"),
                table_out=Identifier(f"{name}_adm{level_max}_01"),
            ),
        )
        conn.execute(
            SQL(drop_tmp).format(
                table_in1=Identifier(f"{name}_adm{level}"),
                table_in2=Identifier(f"{name}_adm{level_max}"),
            ),
        )
        conn.execute(
            SQL(query_4).format(
                table_in=Identifier(f"{name}_adm{level_max}_01"),
                table_out=Identifier(f"{name}_adm{level_max}"),
            ),
        )
    logger.info(name)
