from psycopg2.sql import SQL, Identifier, Literal
from .utils import logging, get_src_ids, get_wld_ids, add_col_query

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT * FROM {table_in};
"""
query_2 = """
    ALTER TABLE {table_out}
    ADD COLUMN IF NOT EXISTS id_attr VARCHAR DEFAULT {id_attr};
"""
query_3 = """
    UPDATE {table_out}
    SET
        adm4_id = COALESCE(adm4_id, adm3_id, adm2_id, adm1_id, adm0_id),
        adm3_id = COALESCE(adm3_id, adm2_id, adm1_id, adm0_id),
        adm2_id = COALESCE(adm2_id, adm1_id, adm0_id),
        adm1_id = COALESCE(adm1_id, adm0_id);
"""
query_4 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON b.adm0_src = a.id_attr
    ORDER BY adm4_id;
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(cur, src, wld, row):
    name = row['id']
    adm0 = row['id_attr']
    lvl = row['lvl']
    cur.execute(SQL(query_1).format(
        table_in=Identifier(f'{src}_{name}_adm{lvl}_voronoi'),
        table_out=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}_tmp1'),
    ))
    cur.execute(SQL(query_2).format(
        id_attr=Literal(adm0),
        table_out=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}_tmp1'),
    ))
    for id in get_src_ids(4):
        cur.execute(SQL(add_col_query(id)).format(
            name=Identifier(id),
            table_out=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}_tmp1'),
        ))
    cur.execute(SQL(query_3).format(
        table_out=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}_tmp1'),
    ))
    cur.execute(SQL(query_4).format(
        table_in1=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}_tmp1'),
        table_in2=Identifier(f'adm0_clip_{wld}'),
        ids_src=SQL(',').join(
            map(lambda x: Identifier('a', x), get_src_ids(4, 1))),
        ids_wld=SQL(',').join(
            map(lambda x: Identifier('b', x), get_wld_ids())),
        table_out=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}'),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{src}_{name}_adm{lvl}_voronoi_{wld}_tmp1'),
    ))
    logger.info(f'{src}_{wld}_{name}')
