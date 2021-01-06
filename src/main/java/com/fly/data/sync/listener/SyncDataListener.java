package com.fly.data.sync.listener;

import com.fly.data.sync.config.SyncDataConfig;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.service.SyncDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.Lifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class SyncDataListener {

    private final SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory;

    private final SyncDataService syncDataService;

    private final List<MessageListenerContainer> messageListenerContainerList = new ArrayList<>();

    public SyncDataListener(@Qualifier("rabbitListenerContainerFactory") SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory, SyncDataService syncDataService) {
        this.rabbitListenerContainerFactory = rabbitListenerContainerFactory;
        this.syncDataService = syncDataService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void syncData() {
        log.info("- begin to sync data...");

        List<String> tableList = SyncDataConfig.getTableList();

        if (tableList.isEmpty()) {
            log.info("- table list is empty, return now...");
            return;
        }

        tableList.forEach(this::syncDataForTable);
    }


    /**
     * 同步指定表的数据
     *
     * @param tableName 表名称
     */
    private <T> void syncDataForTable(String tableName) {
        log.info("- begin to sync data for table: {}", tableName);

        DataModel<T> dataModel = SyncDataConfig.getDataModel(tableName);

        createMessageListener(dataModel);

        syncDataService.syncAll(dataModel);
    }


    /**
     * 创建消息监听器
     *
     * @param dataModel     数据模型
     */
    private <T> void createMessageListener(DataModel<T> dataModel) {
        log.info("- create message listener for model: {}", dataModel.getTable());

        SimpleMessageListenerContainer container = rabbitListenerContainerFactory.createListenerContainer();

        container.setQueueNames("test.queue");
        container.setMessageListener(message -> handleMessage(dataModel, message));
        container.start();

        messageListenerContainerList.add(container);
    }


    /**
     * 处理消息
     *
     * @param dataModel     数据模型
     * @param message       消息
     */
    private <T> void handleMessage(DataModel<T> dataModel, Message message) {
        log.info("- receive message, model: {}, message: {}", dataModel.getTable(), message.getBody());
        syncDataService.syncDelta(dataModel, message);
    }


    /**
     * 销毁消息监听器
     */
    @PreDestroy
    public void stopListenerList() {
        messageListenerContainerList.forEach(Lifecycle::stop);
        log.info("- stop all message listeners...");
    }
}
