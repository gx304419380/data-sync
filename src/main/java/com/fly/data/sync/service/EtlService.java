package com.fly.data.sync.service;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.PageDto;
import com.fly.data.sync.entity.SyncMessage;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
public interface EtlService {

    /**
     * 分页查询指定模型的数据
     *
     * @param model data model
     * @param page  page
     * @param size  size
     * @param <T>   T
     * @return      page
     */
    <T> PageDto<T> page(DataModel<T> model, int page, int size);

    /**
     * 将json转为自定义的SyncMessage
     *
     * @param message json字符串
     * @param <T>       泛型
     * @return          SyncMessage
     */
    <T> SyncMessage<T> convertMessage(String message);
}
