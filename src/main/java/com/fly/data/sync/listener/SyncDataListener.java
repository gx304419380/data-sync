package com.fly.data.sync.listener;

import com.fly.data.sync.config.SyncDataConfig;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.service.SyncDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.Lifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.fly.data.sync.config.SyncDataConfig.getDataModel;

@Component
@Slf4j
public class SyncDataListener {

    private final ApplicationEventPublisher publisher;

    private final SimpleRabbitListenerContainerFactory containerFactory;

    private final SyncDataService syncDataService;

    private final List<MessageListenerContainer> messageListenerContainerList = new ArrayList<>();

    public SyncDataListener(SyncDataService syncDataService,
                            ApplicationEventPublisher publisher,
                            SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory) {

        this.containerFactory = rabbitListenerContainerFactory;
        this.syncDataService = syncDataService;
        this.publisher = publisher;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReadyForSync() {
        log.info("- on ApplicationReadyEvent for sync data...");
        List<String> tableList = SyncDataConfig.getTableList();

        if (tableList.isEmpty()) {
            log.info("- table list is empty, return now...");
            return;
        }

        //创建消息监听器
        tableList.forEach(this::createMessageListener);

        //全量同步数据
        publisher.publishEvent(new SyncAllEvent());
    }


    /**
     * 同步所有的表
     * @param event     事件
     */
    @TransactionalEventListener(fallbackExecution = true)
    public void onSyncAllEvent(SyncAllEvent event) {
        log.info("- on SyncAllEvent...");

        List<String> tableList = SyncDataConfig.getTableList();

        tableList.stream()
                .map(SyncDataConfig::getDataModel)
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

        DataModel<T> dataModel = getDataModel(tableName);

        SimpleMessageListenerContainer container = containerFactory.createListenerContainer();

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
        byte[] body = message.getBody();
        log.info("- receive message, model: {}, message: {}", dataModel.getTable(), body);

        String json = new String(body, StandardCharsets.UTF_8);

        syncDataService.syncDelta(dataModel, json);
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
