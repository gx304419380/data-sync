package com.fly.data.sync.service.impl;

import com.fly.data.sync.annotation.SyncLock;
import com.fly.data.sync.config.SyncDataConfig;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.service.SyncDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
@Slf4j
public class SyncDataServiceImpl implements SyncDataService {
    
    private static final int PAGE_SIZE = 1000;

    @Autowired
    private JdbcTemplate jdbcTemplate;


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

        long totalPage = count / PAGE_SIZE + 1;

        //需要分批次：防止数据量过大内存溢出
        for (int i = 1; i <= totalPage; i++) {
            //获取数据 Extract&Transform
            List<T> dataList = extractAndTransform(model, i, PAGE_SIZE);
            //加载到数据库 Load
            load(dataList, model);
        }

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

    private <T> void load(List<T> dataList, DataModel<T> model) {
        // TODO: 2021/1/6 数据加载进数据库中 
    }

    private <T> long getCount(DataModel<T> model) {
        // TODO: 2021/1/6 查询数据个数
        return 0;
    }
}
