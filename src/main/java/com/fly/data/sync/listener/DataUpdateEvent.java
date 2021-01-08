package com.fly.data.sync.listener;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.core.ResolvableType;

import java.util.List;

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
