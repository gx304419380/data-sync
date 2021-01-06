package com.fly.data.sync.entity;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.util.List;

@Data
@Accessors(chain = true)
@ToString(exclude = "fieldList")
public class DataModel<T> {

    private Class<T> modelClass;

    private String table;

    private String id;

    private List<Field> fieldList;

    private List<String> fieldNameList;

}
