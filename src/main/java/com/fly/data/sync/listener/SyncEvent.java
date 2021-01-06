package com.fly.data.sync.listener;

import com.fly.data.sync.entity.DataModel;
import lombok.Data;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/6
 */
@Data
public class SyncEvent<T> {
    private DataModel<T> dataModel;

    public SyncEvent(DataModel<T> dataModel) {
        this.dataModel = dataModel;
    }
}
