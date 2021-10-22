package com.fly.data.sync.event;

import com.fly.data.sync.constant.SyncOperation;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class DataUpdateEvent<T> extends DataBaseEvent<T> {


    public DataUpdateEvent(UpdateData<T> updateData, DataModel<T> model) {
        this.operation = SyncOperation.UPDATE;
        this.data = updateData.getData();
        this.oldData = updateData.getOldData();
        this.dataModel = model;
    }
}
