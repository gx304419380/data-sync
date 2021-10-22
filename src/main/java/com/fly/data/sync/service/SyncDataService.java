package com.fly.data.sync.service;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.PageDto;
import com.fly.data.sync.entity.UpdateData;
import com.fly.data.sync.listener.DataAddEvent;
import com.fly.data.sync.listener.DataDeleteEvent;
import com.fly.data.sync.listener.DataUpdateEvent;
import com.fly.data.sync.util.SyncCheck;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * Data sync service
 * @author guoxiang
 */
@Service
@Slf4j
public class SyncDataService {


    @Value("${sync.data.page.size:100}")
    private int pageSize;

    private final ModelDao modelDao;

    private final ApplicationEventPublisher publisher;

    private final RestTemplate restTemplate;

    public SyncDataService(ModelDao modelDao,
                           RestTemplate restTemplate,
                           ApplicationEventPublisher publisher) {
        this.modelDao = modelDao;
        this.publisher = publisher;
        this.restTemplate = restTemplate;
    }


    /**
     * 全量同步
     *
     * @param model 数据模型
     */
    @Transactional(rollbackFor = Exception.class)
    public <T> void syncTotal(DataModel<T> model) {
        log.info("- sync all data for model: {}", model.getTable());

        model.getDataLock().lock();

        try {
            //清空临时表
            clearTemporaryTable(model);

            long totalPage = 1;

            //分批次加载数据到临时表：防止数据量过大内存溢出
            for (int i = 1; i <= totalPage; i++) {
                PageDto<T> page = extractAndTransform(model, i, pageSize);

                List<T> dataList = page.getContent();
                long count = page.getTotalElements();
                totalPage = count / pageSize + 1;

                loadToTemporary(dataList, model);
            }

            //加载数据到主表
            loadToTable(model);

        } finally {
            model.getDataLock().unlock();
        }
        log.info("- finish sync all for model: {}", model.getTable());
    }



    /**
     * 增量同步
     * @param model     数据模型
     * @param message   消息
     */
    @Transactional(rollbackFor = Exception.class)
    public <T> void syncDelta(DataModel<T> model, String message) {
        log.info("- sync delta for model: {}", model.getTable());
        model.getDataLock().lock();
        try {
            // TODO: 2021/1/8 增量同步


        } finally {
            model.getDataLock().unlock();
        }

        log.info("- finish sync delta for model: {}", model.getTable());
    }


    /**
     * 创建临时表
     * @param table     原表
     */
    public void createTempTableIfNotExist(String table) {
        log.info("- create temp table for {} if not exists", table);

        modelDao.createTempTableIfNotExist(table);

        log.info("- finish create temp table...");
    }



    /**
     * 清空临时表
     * @param model     数据模型
     */
    private <T> void clearTemporaryTable(DataModel<T> model) {
        modelDao.deleteTemp(model);
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
        //这里会存在一个问题：数据量大内存溢出
        List<T> addList = modelDao.add(model);
        List<T> deleteList = modelDao.delete(model);
        UpdateData<T> updateData = modelDao.update(model);


        //发射数据变更事件
        if (SyncCheck.notEmpty(addList)) {
            log.info("- publish data add event, size = {}", addList.size());
            log.debug("- == add data = {}", addList);
            publisher.publishEvent(new DataAddEvent<>(addList, model));
        }

        if (SyncCheck.notEmpty(deleteList)) {
            log.info("- publish data delete event, size = {}", deleteList.size());
            log.debug("- == delete data = {}", deleteList);
            publisher.publishEvent(new DataDeleteEvent<>(deleteList, model));
        }

        if (updateData.isNotEmpty()) {
            log.info("- publish data update event, size = {}", updateData.size());
            log.debug("- == update data = {}", updateData);
            publisher.publishEvent(new DataUpdateEvent<>(updateData, model));
        }
    }


    /**
     * rest调用分页查询数据，并转为entity
     *
     * @param model     模型
     * @param page      页码
     * @param size      分页大小
     * @return          查询结果
     */
    private <T> PageDto<T> extractAndTransform(DataModel<T> model, long page, long size) {
        // TODO: 2021/1/6 数据查询并转换为pojo
        String url = "http://localhost:8091/device/page?page={1}&size={2}";

        ParameterizedTypeReference<PageDto<T>> reference = getReference(model.getModelClass());
        ResponseEntity<PageDto<T>> responseEntity =
                restTemplate.exchange(url, HttpMethod.GET, null, reference, page, size);
        PageDto<T> body = responseEntity.getBody();
        Assert.notNull(body, "response is null");

        return body;
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private <T> ParameterizedTypeReference<PageDto<T>> getReference(Class<T> clazz) {

        //objectMapper已经缓存Type，不需要额外缓存
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametricType(PageDto.class, clazz);

        return ParameterizedTypeReference.forType(javaType);
    }

}
