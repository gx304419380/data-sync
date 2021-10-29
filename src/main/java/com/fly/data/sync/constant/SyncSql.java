package com.fly.data.sync.constant;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
public class SyncSql {
    private SyncSql() {
    }

    public static final String INSERT_TEMP_SQL = "insert into ${tempTable} (${columnString}) values (${fieldListString})";
    public static final String INSERT_SQL = "insert into ${table} (${columnString}) values (${fieldListString})";

    public static final String QUERY_ADD_SQL = "select ${a.columnList} from ${tempTable} a " +
            "left join ${table} b on a.${idColumn} = b.${idColumn} " +
            "where b.${idColumn} is null";

    public static final String ADD_SQL = "insert into ${table} (${columnString}) " +
            "select ${a.columnList} from ${tempTable} a " +
            "left join ${table} b on a.${idColumn} = b.${idColumn} " +
            "where b.${idColumn} is null";

    public static final String QUERY_UPDATE_SQL = "select ${a.columnList} from ${tempTable} a " +
            "join ${table} b on a.${idColumn} = b.${idColumn} " +
            "where a.${updateTime} > b.${updateTime} or (a.${updateTime} is not null and b.${updateTime} is null)";

    public static final String QUERY_OLD_SQL = "select ${b.columnList} from ${tempTable} a " +
            "left join ${table} b on a.${idColumn} = b.${idColumn} " +
            "where a.${updateTime} > b.${updateTime} or (a.${updateTime} is not null and b.${updateTime} is null)";

    public static final String UPDATE_SQL = "update ${table}, ${tempTable} " +
            "set ${updateSetString} " +
            "where ${table}.${idColumn} = ${tempTable}.${idColumn} " +
            "and ${table}.${updateTime} < ${tempTable}.${updateTime} " +
            "or (${table}.${updateTime} is null and ${tempTable}.${updateTime} is not null)";

    public static final String UPDATE_DELTA_SQL = "update ${table} set ${updateSetDeltaString} " +
            "where ${idColumn}=:${idField} and (${updateTime}<:${updateTimeField} or ${updateTime} is null)";

    public static final String QUERY_DELETE_SQL = "select ${a.columnList} from ${table} a " +
            "left join ${tempTable} b on a.${idColumn} = b.${idColumn} " +
            "where b.${idColumn} is null";

    public static final String DELETE_SQL = "delete a from ${table} a " +
            "left join ${tempTable} b on a.${idColumn} = b.${idColumn} " +
            "where b.${idColumn} is null";

    public static final String DELETE_DELTA_SQL = "delete from ${table} where ${idColumn}=:${idField}";
}
