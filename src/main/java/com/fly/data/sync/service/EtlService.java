package com.fly.data.sync.service;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.PageDto;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
public interface EtlService {

    /**
     * page search
     *
     * @param model data model
     * @param page  page
     * @param size  size
     * @param <T>   T
     * @return      page
     */
    <T> PageDto<T> page(DataModel<T> model, int page, int size);
}
