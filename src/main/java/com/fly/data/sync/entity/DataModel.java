package com.fly.data.sync.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fly.data.sync.annotation.*;
import com.fly.data.sync.util.SyncCheck;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.fly.data.sync.constant.SyncConstant.*;
import static com.fly.data.sync.constant.SyncSql.*;
import static com.fly.data.sync.util.StringConverter.LOWER_CAMEL_UNDERSCORE;
import static com.fly.data.sync.util.StringConverter.UPPER_CAMEL_UNDERSCORE;
import static java.util.stream.Collectors.toList;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/11
 */
@Data
@Accessors(chain = true)
@ToString(of = {"modelClass", "table", "queue", "idColumn", "columnList", "tombstone", "updateTimeColumn"})
public class DataModel<T> {

    private Class<T> modelClass;

    /**
     * lock used for data-sync
     */
    private final ReentrantLock dataLock = new ReentrantLock();

    /**
     * table name
     */
    private String table;

    /**
     * temp table name
     */
    private String tempTable;

    /**
     * queue name
     */
    private String queue;

    /**
     * id property name
     */
    private String idColumn;

    private String idFieldName;

    private Field idField;

    private String updateTimeColumn;

    private String tombstoneColumn;

    private boolean tombstone;

    private String deletedValue;

    private String notDeletedValue;

    private List<Field> fieldList;

    private List<String> columnList;

    private String columnString;

    private String fieldListString;

    /**
     * 全量更新sql
     */
    private String updateSetAllString;

    /**
     * 增量更新sql
     */
    private String updateSetDeltaString;

    private BeanPropertyRowMapper<T> rowMapper;


    // SQL Segment

    /**
     * 全量插入临时表
     */
    private String insertTempSql;

    /**
     * 插入普通表
     */
    private String insertSql;

    /**
     * 根据临时表比对查询新增数据
     */
    private String queryAddSql;

    /**
     * 临时表比对插入
     */
    private String addSql;

    /**
     * 根据临时表比对查询更新数据
     */
    private String queryUpdateSql;

    /**
     * 查询更新前的数据
     */
    private String queryOldSql;

    /**
     * 临时表比对更新
     */
    private String updateAllSql;

    /**
     * 增量更新，普通更新
     */
    private String updateDeltaSql;

    private String queryDeleteSql;

    /**
     * 全量删除，比对临时表删除
     */
    private String deleteAllSql;

    /**
     * 增量删除，普通删除方法
     */
    private String deleteDeltaSql;


    public DataModel(Class<T> modelClass) {

        this.table = resolveTableName(modelClass);
        this.queue = modelClass.getAnnotation(SyncTable.class).queue();

        List<Field> fields = Arrays.stream(modelClass.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .collect(toList());

        this.fieldList = fields.stream()
                .filter(this::checkField)
                .collect(toList());

        this.idField = fields.stream()
                .filter(f -> f.isAnnotationPresent(SyncId.class) || f.isAnnotationPresent(TableId.class))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        idField.setAccessible(true);
        this.idFieldName = idField.getName();
        this.idColumn = resolveTableField(idField);

        this.updateTimeColumn = fieldList.stream()
                .filter(f -> f.isAnnotationPresent(SyncUpdateTime.class))
                .map(this::resolveTableField)
                .findFirst()
                .orElse(UPDATE_TIME_FIELD);

        fieldList.stream()
                .filter(f -> f.isAnnotationPresent(SyncTombstone.class))
                .findFirst()
                .ifPresent(this::resolveTombstone);

        this.tombstone = SyncCheck.notEmpty(tombstoneColumn);

        this.columnList = fieldList.stream()
                .map(this::resolveTableField)
                .filter(SyncCheck::notEmpty)
                .collect(toList());

        List<String> propertyList = fieldList.stream()
                .map(Field::getName)
                .collect(toList());

        this.tempTable = this.table + TEMP_SUFFIX;
        this.modelClass = modelClass;
        this.rowMapper = new BeanPropertyRowMapper<>(modelClass);
        this.columnString = String.join(",", columnList);
        this.fieldListString = ":" + String.join(",:", propertyList);
        this.updateSetAllString = columnList.stream()
                .filter(name -> !name.equals(idColumn))
                .map(name -> table + "." + name + "=" + tempTable + "." + name)
                .collect(Collectors.joining(",", " ", " "));

        StringBuilder s = new StringBuilder();
        for (int i = 0; i < columnList.size(); i++) {
            String column = columnList.get(i);
            Field field = fieldList.get(i);
            if (column.equals(idColumn)) {
                continue;
            }

            s.append(column).append("=:").append(field.getName()).append(",");
        }

        this.updateSetDeltaString = s.substring(0, s.length() - 1);

        this.insertTempSql = parseSql(INSERT_TEMP_SQL);
        this.insertSql = parseSql(INSERT_SQL);
        this.queryAddSql = parseSql(QUERY_ADD_SQL);
        this.addSql = parseSql(ADD_SQL);
        this.queryUpdateSql = parseSql(QUERY_UPDATE_SQL);
        this.queryOldSql = parseSql(QUERY_OLD_SQL);
        this.updateAllSql = parseSql(UPDATE_SQL);
        this.updateDeltaSql = parseSql(UPDATE_DELTA_SQL);
        this.queryDeleteSql = parseSql(QUERY_DELETE_SQL);
        this.deleteAllSql = parseSql(DELETE_SQL);
        this.deleteDeltaSql = parseSql(DELETE_DELTA_SQL);
    }


    /**
     * 反射获取目标的id
     *
     * @param target 目标
     * @return      id
     */
    public Object getIdOf(Object target) {
        try {
            return idField.get(target);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("get id error", e);
        }
    }

    private String getColumnListWithPrefix(String prefix) {
        return prefix + "." + String.join("," + prefix + ".", columnList);
    }


    private boolean checkField(Field field) {
        return !field.isAnnotationPresent(SyncIgnore.class);
    }


    /**
     * 解析表名称
     *
     * @param modelClass    模型类
     * @return              表名称
     */
    private String resolveTableName(Class<T> modelClass) {

        String tableName = null;

        // mybatis plus annotation is prior
        if (modelClass.isAnnotationPresent(TableName.class)) {
            TableName tableNameAnnotation = modelClass.getAnnotation(TableName.class);
            tableName = tableNameAnnotation.value();
        }
        // spring data's annotation is checked then
        else if (modelClass.isAnnotationPresent(Table.class)) {
            Table tableAnnotation = modelClass.getAnnotation(Table.class);
            tableName = tableAnnotation.value();
        }

        if (SyncCheck.notEmpty(tableName)) {
            return tableName;
        }

        // if there isn't any annotation on the class, use class name as default tableName
        SyncTable syncTable = modelClass.getAnnotation(SyncTable.class);
        tableName = syncTable.value();

        if (SyncCheck.notEmpty(tableName)) {
            return tableName;
        }

        return UPPER_CAMEL_UNDERSCORE.convert(modelClass.getSimpleName());
    }


    /**
     * 解析字段名称
     *
     * @param field     模型字段
     * @return          对应的表字段
     */
    private String resolveTableField(Field field) {

        if (field.isAnnotationPresent(SyncIgnore.class)) {
            return null;
        }

        String fieldName = null;

        //mybatis-plus注解优先
        if (field.isAnnotationPresent(TableField.class)) {
            TableField tableField = field.getAnnotation(TableField.class);
            fieldName = tableField.value();
        }
        //mybatis-plus注解TableId
        else if (field.isAnnotationPresent(TableId.class)) {
            TableId tableId = field.getAnnotation(TableId.class);
            fieldName = tableId.value();
        }
        //spring data注解
        else if (field.isAnnotationPresent(Column.class)) {
            Column column = field.getAnnotation(Column.class);
            fieldName = column.value();
        }

        if (SyncCheck.notEmpty(fieldName)) {
            return fieldName;
        }

        return LOWER_CAMEL_UNDERSCORE.convert(field.getName());
    }

    /**
     * 解析tombstone注解
     * @param field field
     */
    private void resolveTombstone(Field field) {
        SyncTombstone syncTombstone = field.getAnnotation(SyncTombstone.class);
        this.tombstoneColumn = resolveTableField(field);
        this.deletedValue = syncTombstone.deleteValue();
        this.notDeletedValue = syncTombstone.existValue();
    }

    private String parseSql(String sql) {
        return sql.replace("${idColumn}", this.getIdColumn())
                .replace("${idField}", this.getIdFieldName())
                .replace("${table}", this.getTable())
                .replace("${tempTable}", this.getTempTable())
                .replace("${updateTime}", this.getUpdateTimeColumn())
                .replace("${columnString}", this.getColumnString())
                .replace("${fieldListString}", this.getFieldListString())
                .replace("${a.columnList}", this.getColumnListWithPrefix("a"))
                .replace("${b.columnList}", this.getColumnListWithPrefix("b"))
                .replace("${updateSetString}", this.getUpdateSetAllString())
                .replace("${updateSetDeltaString}", this.getUpdateSetDeltaString());
    }

}
