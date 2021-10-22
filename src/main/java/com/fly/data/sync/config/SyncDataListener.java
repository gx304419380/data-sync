package com.fly.data.sync.config;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.event.SyncAllEvent;
import com.fly.data.sync.event.SyncEvent;
import com.fly.data.sync.service.SyncDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.Lifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.event.TransactionalEventListener;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.fly.data.sync.constant.SyncEventSource.APPLICATION_START;

@Slf4j
@RequiredArgsConstructor
public class SyncDataListener {

    private final ApplicationEventPublisher publisher;

    private final SimpleRabbitListenerContainerFactory containerFactory;

    private final SyncDataService syncDataService;

    private final SyncDataContext syncDataContext;

    private final List<MessageListenerContainer> messageListenerContainerList = new ArrayList<>();



    /**
     * 项目启动监听器，用于创建mq listener和启动同步
     */
    @EventListener(ApplicationReadyEvent.class)
    @Async
    public void onApplicationReadyForSync() {
        log.info("- on ApplicationReadyEvent for sync data...");
        List<String> tableList = syncDataContext.getTableList();

        if (tableList.isEmpty()) {
            log.info("- table list is empty, return now...");
            return;
        }

        //创建消息监听器
        tableList.forEach(this::createMessageListener);

        //创建临时表
        tableList.forEach(this::createTempTable);

        //全量同步数据
        publisher.publishEvent(new SyncAllEvent(APPLICATION_START));
    }



    /**
     * 同步所有的表
     * @param event     事件
     */
    @TransactionalEventListener(fallbackExecution = true)
    public void onSyncAllEvent(SyncAllEvent event) {
        log.info("- on SyncAllEvent: {}", event);

        List<String> tableList = syncDataContext.getTableList();

        tableList.stream()
                .map(syncDataContext::getDataModel)
                .forEach(syncDataService::syncTotal);

        log.info("- finish SyncAllEvent...");
    }


    /**
     * 监听单个数据模型事件
     *
     * @param event     事件
     */
    @TransactionalEventListener(fallbackExecution = true)
    public <T> void onSyncEvent(SyncEvent<T> event) {
        DataModel<T> dataModel = event.getDataModel();
        log.info("- on SyncEvent for model: {}", dataModel.getTable());

        syncDataService.syncTotal(dataModel);

        log.info("- finish SyncEvent for model: {}", dataModel.getTable());
    }


    /**
     * 创建消息监听器
     *
     * @param tableName     数据表名
     */
    private <T> void createMessageListener(String tableName) {
        log.info("- create message listener for model: {}", tableName);

        DataModel<T> dataModel = syncDataContext.getDataModel(tableName);

        SimpleMessageListenerContainer container = containerFactory.createListenerContainer();

        container.setQueueNames(dataModel.getQueue());
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
        byte[] body = message.getBody();
        log.info("- receive message, model: {}, message: {}", dataModel.getTable(), body);

        String json = new String(body, StandardCharsets.UTF_8);

        syncDataService.syncDelta(dataModel, json);
    }


    /**
     * 创建临时表
     *
     * @param table table name
     */
    private void createTempTable(String table) {
        syncDataService.createTempTableIfNotExist(table);
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