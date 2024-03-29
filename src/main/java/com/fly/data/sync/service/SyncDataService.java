package com.fly.data.sync.service;

import com.fly.data.sync.config.SyncDataContext;
import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.entity.*;
import com.fly.data.sync.event.DataAddEvent;
import com.fly.data.sync.event.DataDeleteEvent;
import com.fly.data.sync.event.DataUpdateEvent;
import com.fly.data.sync.util.SyncCheck;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.fly.data.sync.constant.SyncConstant.*;

/**
 * Data sync service
 * @author guoxiang
 */
@Slf4j
@RequiredArgsConstructor
public class SyncDataService {


    @Value("${sync.data.page.size:400}")
    private int pageSize;

    private final ModelDao modelDao;

    private final ApplicationEventPublisher publisher;

    private final EtlService etlService;

    private final SyncDataContext syncDataContext;


    /**
     * 全量同步
     *
     * @param model 数据模型
     */
    @Transactional(rollbackFor = Exception.class)
    public <T> void syncTotal(DataModel<T> model) {

        model.getDataLock().lock();

        log.info("- sync all data for model: {}", model.getTable());
        try {
            //清空临时表
            clearTemporaryTable(model);

            long totalPage = 1;

            //分批次加载数据到临时表：防止数据量过大内存溢出
            for (int i = 1; i <= totalPage; i++) {
                PageDto<T> page = extractAndTransform(model, i, pageSize);

                List<T> dataList = page.getRecords();
                long total = page.getTotal();
                totalPage = total / pageSize + 1;

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
     * @param message   消息
     */
    @Transactional(rollbackFor = Exception.class)
    public <T> void syncDelta(String message) {
        log.info("- sync delta message: {}", message);

        SyncMessage<T> syncMessage = etlService.convertMessage(message);

        String table = syncMessage.getTable();
        DataModel<T> model = syncDataContext.getDataModel(table);

        model.getDataLock().lock();
        log.info("- get lock success, begin to sync delta");
        try {
            handleSyncMessage(model, syncMessage);
        } finally {
            model.getDataLock().unlock();
        }

        log.info("- finish sync delta for model: {}", model.getTable());
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
     * delta sync message handle
     *
     * @param model     model
     * @param message   消息
     * @param <T>   <T>
     */
    private <T> void handleSyncMessage(DataModel<T> model, SyncMessage<T> message) {

        List<T> data = message.getData();
        List<Object> idList = message.getIdList();

        switch (message.getType()) {
            case ADD:
            case UPDATE:
                SaveOrUpdateResult<T> result = modelDao.saveOrUpdateDelta(model, idList, data);
                if (result.hasAddList()) {
                    publisher.publishEvent(new DataAddEvent<>(result.getAddList(), model));
                }

                if (result.hasUpdateData()) {
                    publisher.publishEvent(new DataUpdateEvent<>(result.getUpdateData(), model));
                }
                break;
            case DELETE:
                List<T> deleteData = modelDao.deleteDelta(model, idList);
                publisher.publishEvent(new DataDeleteEvent<>(deleteData, model));
                break;
            default:
                log.warn("not supported type: {}", message);
        }
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
     * rest调用分页查询数据，并转为entity
     *
     * @param model     模型
     * @param page      页码
     * @param size      分页大小
     * @return          查询结果
     */
    public <T> PageDto<T> extractAndTransform(DataModel<T> model, int page, int size) {
        log.info("- get page from data center, page = {}, size = {}, table = {}", page, size, model.getTable());

        PageDto<T> result = etlService.page(model, page, size);

        log.debug("- result = {}", result);
        return result;
    }

}
