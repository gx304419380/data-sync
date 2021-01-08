package com.fly.data.sync.listener;

import com.fly.data.sync.entity.DataModel;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
public class DataDeleteEvent<T> extends DataBaseEvent<T> {

    public DataDeleteEvent(List<T> data, DataModel<T> dataModel) {
        this.operation = SyncOperation.DELETE;
        this.data = data;
        this.oldData = data;
        this.dataModel = dataModel;
    }

}
