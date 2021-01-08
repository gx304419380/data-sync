package com.fly.data.sync.listener;

import com.fly.data.sync.entity.DataModel;
import lombok.Data;
import org.springframework.core.ResolvableType;
import org.springframework.core.ResolvableTypeProvider;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
@Data
public class DataBaseEvent<T> implements ResolvableTypeProvider {

    protected SyncOperation operation;

    protected List<T> data;

    protected List<T> oldData;

    protected DataModel<T> dataModel;



    public DataBaseEvent() {
    }

    @Override
    public ResolvableType getResolvableType() {
        Class<T> dataClass  = dataModel.getModelClass();
        return ResolvableType.forClassWithGenerics(getClass(), ResolvableType.forClass(dataClass));
    }
}
