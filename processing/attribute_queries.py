q0 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT b.*, a.geom
    FROM {table_in} AS a
    LEFT JOIN adm0_attributes AS b ON a.adm0_id = b.adm0_id
    ORDER BY a.adm0_id;
"""
q1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT c.*, b.*, a.geom
    FROM {table_in} AS a
    LEFT JOIN adm0_attributes AS b ON a.adm0_id = b.adm0_id
    LEFT JOIN adm1_attributes AS c ON a.adm1_id = c.adm1_id
    ORDER BY a.adm1_id, a.adm0_id;
"""
q2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT d.*, c.*, b.*, a.geom
    FROM {table_in} AS a
    LEFT JOIN adm0_attributes AS b ON a.adm0_id = b.adm0_id
    LEFT JOIN adm1_attributes AS c ON a.adm1_id = c.adm1_id
    LEFT JOIN adm2_attributes AS d ON a.adm2_id = d.adm2_id
    ORDER BY a.adm2_id, a.adm1_id, a.adm0_id;
"""
q3 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT e.*, d.*, c.*, b.*, a.geom
    FROM {table_in} AS a
    LEFT JOIN adm0_attributes AS b ON a.adm0_id = b.adm0_id
    LEFT JOIN adm1_attributes AS c ON a.adm1_id = c.adm1_id
    LEFT JOIN adm2_attributes AS d ON a.adm2_id = d.adm2_id
    LEFT JOIN adm3_attributes AS e ON a.adm3_id = e.adm3_id
    ORDER BY a.adm3_id, a.adm2_id, a.adm1_id, a.adm0_id;
"""
q4 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT f.*, e.*, d.*, c.*, b.*, a.geom
    FROM {table_in} AS a
    LEFT JOIN adm0_attributes AS b ON a.adm0_id = b.adm0_id
    LEFT JOIN adm1_attributes AS c ON a.adm1_id = c.adm1_id
    LEFT JOIN adm2_attributes AS d ON a.adm2_id = d.adm2_id
    LEFT JOIN adm3_attributes AS e ON a.adm3_id = e.adm3_id
    LEFT JOIN adm4_attributes AS f ON a.adm4_id = f.adm4_id
    ORDER BY a.adm4_id, a.adm3_id, a.adm2_id, a.adm1_id, a.adm0_id;
"""
q5 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT g.*, f.*, e.*, d.*, c.*, b.*, a.geom
    FROM {table_in} AS a
    LEFT JOIN adm0_attributes AS b ON a.adm0_id = b.adm0_id
    LEFT JOIN adm1_attributes AS c ON a.adm1_id = c.adm1_id
    LEFT JOIN adm2_attributes AS d ON a.adm2_id = d.adm2_id
    LEFT JOIN adm3_attributes AS e ON a.adm3_id = e.adm3_id
    LEFT JOIN adm4_attributes AS f ON a.adm4_id = f.adm4_id
    LEFT JOIN adm5_attributes AS g ON a.adm5_id = g.adm5_id
    ORDER BY a.adm5_id, a.adm4_id, a.adm3_id, a.adm2_id, a.adm1_id, a.adm0_id;
"""
queries = {0: q0, 1: q1, 2: q2, 3: q3, 4: q4, 5: q5}
