-- TABLE temp_table: table creation sql command based on database MySql

CREATE TABLE temp_table(
    TABLE_AFFILIATION VARCHAR(6) COMMENT '归属地',
    MINDATE VARCHAR(10),
    MAXDATE VARCHAR(10),
    START_ODMS_DATE VARCHAR(8),
    TO_INCT_DATE VARCHAR(8)
) COMMENT '临时表，可删除';
