from psycopg import connect
from psycopg.sql import SQL, Identifier, Literal

from .utils import (
    DATABASE,
    add_col_query,
    adm0_list,
    geoms,
    get_src_ids,
    get_wld_ids,
    logging,
)

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT * FROM {table_in}
    WHERE adm0_src IN ({ids});
"""
query_2 = """
    UPDATE {table_out}
    SET
        adm4_id = COALESCE(adm4_id, adm3_id, adm2_id, adm1_id, adm0_id),
        adm3_id = COALESCE(adm3_id, adm2_id, adm1_id, adm0_id),
        adm2_id = COALESCE(adm2_id, adm1_id, adm0_id),
        adm1_id = COALESCE(adm1_id, adm0_id);
"""
query_3 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in1} AS a;
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(dest, wld):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for geom in geoms:
        for l in range(1, 5):
            conn.execute(
                SQL(drop_tmp).format(
                    table_tmp1=Identifier(f"{dest}_adm{l}_{geom}_{wld}"),
                ),
            )
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"{dest}_adm0_polygons_{wld}"),
            ids=SQL(",").join(
                map(lambda x: Literal(x.upper()), adm0_list[f"{dest}_{wld}"]),
            ),
            table_out=Identifier(f"{dest}_adm4_polygons_{wld}_tmp1"),
        ),
    )
    for id in get_src_ids(4):
        conn.execute(
            SQL(add_col_query(id)).format(
                name=Identifier(id),
                table_out=Identifier(f"{dest}_adm4_polygons_{wld}_tmp1"),
            ),
        )
    conn.execute(
        SQL(query_2).format(
            table_out=Identifier(f"{dest}_adm4_polygons_{wld}_tmp1"),
        ),
    )
    conn.execute(
        SQL(query_3).format(
            table_in1=Identifier(f"{dest}_adm4_polygons_{wld}_tmp1"),
            ids_src=SQL(",").join(map(lambda x: Identifier("a", x), get_src_ids(4))),
            ids_wld=SQL(",").join(
                map(lambda x: Identifier("a", x), get_wld_ids(False)),
            ),
            id=Identifier("adm4_id"),
            table_out=Identifier(f"{dest}_adm4_polygons_{wld}"),
        ),
    )
    for l in range(3, 0, -1):
        conn.execute(
            SQL(query_3).format(
                table_in1=Identifier(f"{dest}_adm{l + 1}_polygons_{wld}"),
                ids_src=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_src_ids(l)),
                ),
                ids_wld=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_wld_ids(False)),
                ),
                id=Identifier(f"adm{l}_id"),
                table_out=Identifier(f"{dest}_adm{l}_polygons_{wld}"),
            ),
        )
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"{dest}_adm4_polygons_{wld}_tmp1"),
        ),
    )
    conn.close()
    logger.info(f"{dest}_{wld}")
