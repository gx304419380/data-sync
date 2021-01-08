package com.fly.data.sync.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fly.data.sync.annotation.SyncUpdateTime;
import com.fly.data.sync.annotation.SyncIgnore;
import com.fly.data.sync.annotation.SyncTable;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fly.data.sync.constant.SyncConstant.*;
import static com.fly.data.sync.util.StringConverter.LOWER_CAMEL_UNDERSCORE;
import static com.fly.data.sync.util.StringConverter.UPPER_CAMEL_UNDERSCORE;
import static java.util.stream.Collectors.toList;

@Data
@Accessors(chain = true)
@ToString(exclude = {"fieldList", "rowMapper"})
public class DataModel<T> {

    private Class<T> modelClass;

    private final ReentrantLock dataLock = new ReentrantLock();

    private String table;

    private String tempTable;

    private String id;

    private String updateTime;

    private List<Field> fieldList;

    private List<String> fieldNameList;

    private String fieldNameListString;

    private String propertyNameString;

    private String updateFieldString;

    private BeanPropertyRowMapper<T> rowMapper;


    public DataModel(Class<T> modelClass) {

        String tableName = resolveTableName(modelClass);

        Field[] fields = modelClass.getDeclaredFields();

        List<Field> fieldList = Stream.of(fields)
                .filter(this::checkField)
                .collect(Collectors.toList());

        String idName = fieldList.stream()
                .filter(f -> f.isAnnotationPresent(TableId.class))
                .map(this::resolveTableField)
                .findFirst()
                .orElse(ID_FIELD);

        String updateTime = fieldList.stream()
                .filter(f -> f.isAnnotationPresent(SyncUpdateTime.class))
                .map(this::resolveTableField)
                .findFirst()
                .orElse(UPDATE_TIME_FIELD);

        List<String> fieldNameList = fieldList.stream()
                .map(this::resolveTableField)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());

        List<String> propertyList = fieldList.stream()
                .map(Field::getName)
                .collect(toList());

        this.id = idName;
        this.table = tableName;
        this.tempTable = tableName + TEMP_SUFFIX;
        this.updateTime = updateTime;
        this.fieldList = fieldList;
        this.modelClass = modelClass;
        this.fieldNameList = fieldNameList;
        this.rowMapper = new BeanPropertyRowMapper<>(modelClass);
        this.fieldNameListString = String.join(",", fieldNameList);
        this.propertyNameString = ":" + String.join(",:", propertyList);
        this.updateFieldString = fieldNameList.stream()
                .filter(name -> !name.equals(id))
                .map(name -> table + "." + name + "=" + tempTable + "." + name)
                .collect(Collectors.joining(",", " ", " "));
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

        if (StringUtils.isNotEmpty(tableName)) {
            return tableName;
        }

        // if there isn't any annotation on the class, use class name as default tableName
        SyncTable syncTable = modelClass.getAnnotation(SyncTable.class);
        tableName = syncTable.value();

        if (StringUtils.isNotEmpty(tableName)) {
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

        if (StringUtils.isNotEmpty(fieldName)) {
            return fieldName;
        }

        return LOWER_CAMEL_UNDERSCORE.convert(field.getName());
    }

}
