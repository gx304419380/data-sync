package com.fly.data.sync.entity;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

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

}
