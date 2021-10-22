package com.fly.data.sync.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fly.data.sync.annotation.SyncTombstone;
import com.fly.data.sync.annotation.SyncUpdateTime;
import com.fly.data.sync.annotation.SyncIgnore;
import com.fly.data.sync.annotation.SyncTable;
import com.fly.data.sync.util.SyncCheck;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
@ToString(of = {"modelClass", "table", "queue", "id", "fieldNameList", "tombstone", "updateTime"})
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
    private String id;

    private String updateTime;

    private String tombstoneField;

    private boolean tombstone;

    private String tombstoneDeleteValue;

    private String tombstoneExistValue;

    private List<Field> fieldList;

    private List<String> fieldNameList;

    private String fieldNameListString;

    private String propertyNameString;

    private String updateFieldString;

    private BeanPropertyRowMapper<T> rowMapper;


    // SQL Segment

    private String insertSql;

    private String tombstoneSql;

    private String queryAddSql;

    private String addSql;

    private String queryUpdateSql;

    private String queryOldSql;

    private String updateSql;

    private String queryDeleteSql;

    private String deleteSql;


    public DataModel(Class<T> modelClass) {

        this.table = resolveTableName(modelClass);
        this.queue = modelClass.getAnnotation(SyncTable.class).queue();

        Field[] fields = modelClass.getDeclaredFields();

        this.fieldList = Stream.of(fields)
                .filter(this::checkField)
                .collect(toList());

        this.id = fieldList.stream()
                .filter(f -> f.isAnnotationPresent(TableId.class))
                .map(this::resolveTableField)
                .findFirst()
                .orElse(ID_FIELD);

        this.updateTime = fieldList.stream()
                .filter(f -> f.isAnnotationPresent(SyncUpdateTime.class))
                .map(this::resolveTableField)
                .findFirst()
                .orElse(UPDATE_TIME_FIELD);

        fieldList.stream()
                .filter(f -> f.isAnnotationPresent(SyncTombstone.class))
                .findFirst()
                .ifPresent(this::resolveTombstone);

        this.tombstone = SyncCheck.notEmpty(tombstoneField);

        this.fieldNameList = fieldList.stream()
                .map(this::resolveTableField)
                .filter(SyncCheck::notEmpty)
                .collect(toList());

        List<String> propertyList = fieldList.stream()
                .map(Field::getName)
                .collect(toList());

        this.tempTable = this.table + TEMP_SUFFIX;
        this.modelClass = modelClass;
        this.rowMapper = new BeanPropertyRowMapper<>(modelClass);
        this.fieldNameListString = String.join(",", fieldNameList);
        this.propertyNameString = ":" + String.join(",:", propertyList);
        this.updateFieldString = fieldNameList.stream()
                .filter(name -> !name.equals(id))
                .map(name -> table + "." + name + "=" + tempTable + "." + name)
                .collect(Collectors.joining(",", " ", " "));

        this.insertSql = parseSql(INSERT_SQL);
        this.queryAddSql = parseSql(QUERY_ADD_SQL);
        this.addSql = parseSql(ADD_SQL);
        this.queryUpdateSql = parseSql(QUERY_UPDATE_SQL);
        this.queryOldSql = parseSql(QUERY_OLD_SQL);
        this.updateSql = parseSql(UPDATE_SQL);
        this.queryDeleteSql = parseSql(QUERY_DELETE_SQL);
        this.deleteSql = parseSql(DELETE_SQL);
    }


    public String getFieldNameListStringWithPrefix(String prefix) {
        return prefix + "." + String.join("," + prefix + ".", fieldNameList);
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
        this.tombstoneField = resolveTableField(field);
        this.tombstoneDeleteValue = syncTombstone.deleteValue();
        this.tombstoneExistValue = syncTombstone.existValue();
    }

    private String parseSql(String sql) {
        return sql.replace("${id}", this.getId())
                .replace("${table}", this.getTable())
                .replace("${tempTable}", this.getTempTable())
                .replace("${updateTime}", this.getUpdateTime())
                .replace("${fieldList}", this.getFieldNameListString())
                .replace("${propertyList}", this.getPropertyNameString())
                .replace("${a.fieldList}", this.getFieldNameListStringWithPrefix("a"))
                .replace("${b.fieldList}", this.getFieldNameListStringWithPrefix("b"))
                .replace("${updateField}", this.getUpdateFieldString());
    }

}
