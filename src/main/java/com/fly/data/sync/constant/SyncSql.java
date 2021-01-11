package com.fly.data.sync.constant;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
public class SyncSql {
    private SyncSql() {
    }

    public static final String INSERT_SQL = "insert into ${tempTable} (${fieldList}) values (${propertyList})";

    public static final String QUERY_ADD_SQL = "select ${a.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where b.${id} is null";

    public static final String ADD_SQL = "insert into ${table} (${fieldList}) " +
            "select ${a.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where b.${id} is null";

    public static final String QUERY_UPDATE_SQL = "select ${a.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where a.${updateTime} > b.${updateTime}";

    public static final String QUERY_OLD_SQL = "select ${b.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where a.${updateTime} > b.${updateTime}";

    public static final String UPDATE_SQL = "update ${table}, ${tempTable} " +
            "set ${updateField} " +
            "where ${table}.${id} = ${tempTable}.${id} " +
            "and ${table}.${updateTime} < ${tempTable}.${updateTime}";

    public static final String QUERY_DELETE_SQL = "select ${a.fieldList} from ${table} a " +
            "left join ${tempTable} b on a.${id} = b.${id} " +
            "where b.${id} is null";

    public static final String DELETE_SQL = "delete a from ${table} a " +
            "left join ${tempTable} b on a.${id} = b.${id} " +
            "where b.${id} is null";
}
