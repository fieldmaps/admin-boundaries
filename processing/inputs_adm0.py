import subprocess
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier, Literal
from .utils import adm0, logging

logger = logging.getLogger(__name__)

geometries = {
    'adm0_polygons': ('MultiPolygon', 3),
    'adm0_lines': ('MultiLineString', 2),
    'adm0_points': ('MultiPoint', 1),
}


def main(name, file):
    logger.info(f'Starting {name}')
    subprocess.run([
        'ogr2ogr',
        '-overwrite',
        '--config', 'OGR_GEOJSON_MAX_OBJ_SIZE', '2048MB',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'LAUNDER=NO',
        '-lco', 'SPATIAL_INDEX=NONE',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-nln', f'{name}_tmp1',
        '-f', 'PostgreSQL', 'PG:dbname=edge_matcher',
        file,
    ])
    con = connect(database='edge_matcher')
    cur = con.cursor()
    query_1 = """
        ALTER TABLE {table_in} DROP COLUMN IF EXISTS fid;
        ALTER TABLE {table_in} ADD COLUMN IF NOT EXISTS {adm0_id} VARCHAR;
    """
    query_2 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT
            {adm0_fid} AS adm0_fid,
            COALESCE({adm0_id}, {adm0_fid}) AS adm0_id,
            ST_Transform(ST_Multi(
                ST_CollectionExtract(ST_MakeValid(
                    ST_Force2D(ST_SnapToGrid(geom, 0.000000001))
                ), {geom_code})
            ), 4326)::GEOMETRY({geom_name}, 4326) AS geom
        FROM {table_in};
    """
    query_3 = """
        ALTER TABLE {table_in} DROP COLUMN IF EXISTS geom;
        ALTER TABLE {table_in} DROP COLUMN IF EXISTS adm0_id;
    """
    query_4 = """
        DROP TABLE IF EXISTS {table_out};
        CREATE TABLE {table_out} AS
        SELECT b.*, a.adm0_id, a.geom
        FROM {table_in1} AS a
        JOIN {table_in2} AS b
        ON a.adm0_fid = b.adm0_fid
        ORDER BY a.adm0_fid;
    """
    drop_tmp = """
        DROP TABLE IF EXISTS {table_tmp1};
        DROP TABLE IF EXISTS {table_tmp2};
    """
    geom_name, geom_code = geometries[name]
    cur.execute(SQL(query_1).format(
        adm0_id=Identifier(adm0['adm0_id']),
        table_in=Identifier(f'{name}_tmp1'),
    ))
    cur.execute(SQL(query_2).format(
        adm0_fid=Identifier(adm0['adm0_fid']),
        adm0_id=Identifier(adm0['adm0_id']),
        geom_code=Literal(geom_code),
        geom_name=Identifier(geom_name),
        table_in=Identifier(f'{name}_tmp1'),
        table_out=Identifier(f'{name}_tmp2'),
    ))
    cur.execute(SQL(query_3).format(
        table_in=Identifier(f'{name}_tmp1'),
    ))
    cur.execute(SQL(query_4).format(
        table_in1=Identifier(f'{name}_tmp2'),
        table_in2=Identifier(f'{name}_tmp1'),
        table_out=Identifier(name),
    ))
    cur.execute(SQL(drop_tmp).format(
        table_tmp1=Identifier(f'{name}_tmp1'),
        table_tmp2=Identifier(f'{name}_tmp2'),
    ))
    con.commit()
    cur.close()
    con.close()
    logger.info(f'Finished {name}')
