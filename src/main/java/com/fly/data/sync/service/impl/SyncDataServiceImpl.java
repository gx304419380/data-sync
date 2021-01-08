package com.fly.data.sync.service.impl;

import com.fly.data.sync.annotation.SyncLock;
import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import com.fly.data.sync.listener.DataAddEvent;
import com.fly.data.sync.listener.DataDeleteEvent;
import com.fly.data.sync.listener.DataUpdateEvent;
import com.fly.data.sync.service.SyncDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
@Slf4j
public class SyncDataServiceImpl implements SyncDataService {
    
    private static final int PAGE_SIZE = 1000;

    private final ModelDao modelDao;

    private final ApplicationEventPublisher publisher;

    public SyncDataServiceImpl(ModelDao modelDao, ApplicationEventPublisher publisher) {
        this.modelDao = modelDao;
        this.publisher = publisher;
    }


    /**
     * 全量同步
     *
     * @param model 数据模型
     */
    @Override
    @SyncLock
    public <T> void syncTotal(DataModel<T> model) {
        log.info("- sync all data for model: {}", model.getTable());

        //获取总数
        long count = getCount(model);
        log.info("- data count: {}", count);

        if (count == 0) {
            log.info("- {} data count = 0, return...", model.getTable());
            return;
        }

        long totalPage = count / PAGE_SIZE + 1;

        //清空临时表
        clearTemporaryTable(model);

        //分批次加载数据到临时表：防止数据量过大内存溢出
        for (int i = 1; i <= totalPage; i++) {
            List<T> dataList = extractAndTransform(model, i, PAGE_SIZE);
            loadToTemporary(dataList, model);
        }

        //加载数据到主表
        loadToTable(model);

        log.info("- finish sync all for model: {}", model.getTable());
    }



    /**
     * 增量同步
     * @param model     数据模型
     * @param message   消息
     */
    @Override
    @SyncLock
    public <T> void syncDelta(DataModel<T> model, String message) {
        log.info("- sync delta for model: {}", model.getTable());



        log.info("- finish sync delta for model: {}", model.getTable());
    }


    private <T> List<T> extractAndTransform(DataModel<T> model, long page, long size) {
        // TODO: 2021/1/6 数据查询并转换为pojo




        return Collections.emptyList();
    }


    /**
     * 清空临时表
     * @param model     数据模型
     */
    private <T> void clearTemporaryTable(DataModel<T> model) {
        modelDao.truncateTemp(model);
    }


    /**
     * 加载数据到临时表
     * @param dataList  数据
     * @param model     数据模型
     */
    private <T> void loadToTemporary(List<T> dataList, DataModel<T> model) {
        log.info("- load data to temp table for: {}, size = {}", model.getTable(), dataList.size());
        log.debug("- model data list = {}", dataList);

        //数据加载进临时表中
        modelDao.loadToTemp(dataList, model);
    }


    /**
     * 加载数据到主表
     *
     * @param model     数据模型
     */
    private <T> void loadToTable(DataModel<T> model) {
        log.debug("- load data to table for model: {}", model.getTable());

        //新增、修改、删除并将对应的数据返回
        // TODO: 2021/1/7 这里会存在一个问题：数据量大内存溢出
        List<T> addList = modelDao.add(model);
        List<T> deleteList = modelDao.delete(model);
        UpdateData<T> updateData = modelDao.update(model);


        //发射数据变更事件
        if (CollectionUtils.isNotEmpty(addList)) {
            log.info("- publish data add event, size = {}", addList.size());
            publisher.publishEvent(new DataAddEvent<>(addList, model));
        }

        if (CollectionUtils.isNotEmpty(deleteList)) {
            log.info("- publish data delete event, size = {}", deleteList.size());
            publisher.publishEvent(new DataDeleteEvent<>(deleteList, model));
        }

        if (updateData.isNotEmpty()) {
            log.info("- publish data update event, size = {}", updateData.size());
            publisher.publishEvent(new DataUpdateEvent<>(updateData, model));
        }
    }



    private <T> long getCount(DataModel<T> model) {
        // TODO: 2021/1/6 查询数据个数


        return 0;
    }
}
