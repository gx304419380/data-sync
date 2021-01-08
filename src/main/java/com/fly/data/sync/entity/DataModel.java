package com.fly.data.sync.entity;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Data
@Accessors(chain = true)
@ToString(exclude = "fieldList")
public class DataModel<T> {

    private Class<T> modelClass;

    private final ReentrantLock dataLock = new ReentrantLock();

    private String table;

    private String id;

    private List<Field> fieldList;

    private List<String> fieldNameList;

    private String fieldNameListString;

    private List<String> propertyNameList;

    private BeanPropertyRowMapper<T> rowMapper;

    public DataModel<T> setModelClass(Class<T> modelClass) {
        this.modelClass = modelClass;
        this.rowMapper = new BeanPropertyRowMapper<>(modelClass);
        return this;
    }

    public DataModel<T> setFieldNameList(List<String> fieldNameList) {
        this.fieldNameList = fieldNameList;
        this.fieldNameListString = String.join(",", fieldNameList);
        return this;
    }

    public DataModel<T> setFieldList(List<Field> fieldList) {
        this.fieldList = fieldList;
        this.propertyNameList = fieldList.stream().map(Field::getName).collect(toList());
        return this;
    }

    public String getFieldNameListStringWithPrefix(String prefix) {
        return prefix + "." + String.join("," + prefix + ".", fieldNameList);
    }

    public String getTempTable() {
        return table + "_temp";
    }
}
