package com.fly.data.sync.dao;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
public interface ModelDao {
    <T> void loadToTemp(List<T> dataList, DataModel<T> model);

    <T> List<T> add(DataModel<T> model);

    <T> UpdateData<T> update(DataModel<T> model);

    <T> List<T> delete(DataModel<T> model);

    <T> void truncateTemp(DataModel<T> model);

    void createTempTableIfNotExist(String table);
}
