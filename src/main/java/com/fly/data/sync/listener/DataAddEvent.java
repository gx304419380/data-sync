package com.fly.data.sync.listener;

import com.fly.data.sync.constant.SyncOperation;
import com.fly.data.sync.entity.DataModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.springframework.core.ResolvableType;

import java.util.List;

/**
 * 数据变更事件
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class DataAddEvent<T> extends DataBaseEvent<T> {

    {
        this.operation = SyncOperation.ADD;
    }

    public DataAddEvent(List<T> data, DataModel<T> dataModel) {
        this.data = data;
        this.dataModel = dataModel;
    }


    @Override
    public ResolvableType getResolvableType() {
        Class<T> dataClass  = dataModel.getModelClass();

        return ResolvableType.forClassWithGenerics(getClass(), ResolvableType.forClass(dataClass));
    }
}
