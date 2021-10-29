package com.fly.data.sync.config;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.event.SyncAllEvent;
import com.fly.data.sync.event.SyncEvent;
import com.fly.data.sync.service.SyncDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.Lifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.event.TransactionalEventListener;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.fly.data.sync.constant.SyncEventSource.APPLICATION_START;
import static com.fly.data.sync.util.SyncCheck.notBlank;

@Slf4j
@RequiredArgsConstructor
public class SyncDataListener {

    @Value("${sync.data.exchange:sync.data.exchange}")
    private String exchangeName;

    @Value("${sync.data.queue:}")
    private String queueName;

    @Value("${sync.data.cluster: false}")
    private boolean isCluster;


    /**
     * 是否定义了queue名称，
     * 如果配置queue名称，
     * 则项目停止后不关闭queue
     */
    private boolean configQueue;

    private final ApplicationEventPublisher publisher;

    private final SimpleRabbitListenerContainerFactory containerFactory;

    private final AmqpAdmin rabbitAdmin;

    private final SyncDataService syncDataService;

    private final SyncDataContext syncDataContext;

    private final List<MessageListenerContainer> messageListenerContainerList = new ArrayList<>();

    @PostConstruct
    public void init() {
        configQueue = notBlank(queueName);
        queueName = configQueue ? queueName : "sync.data.queue." + UUID.randomUUID();
    }


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
        createMessageListener();

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
     */
    private void createMessageListener() {
        log.info("- create message listener");

        //判断当前节点是否是主节点，若不是，则退出
        if (!isLeader()) {
            return;
        }


        //生成队列、交换机和绑定规则
        Queue queue = new Queue(queueName);
        FanoutExchange exchange = new FanoutExchange(exchangeName);
        Binding binding = BindingBuilder.bind(queue).to(exchange);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareExchange(exchange);
        rabbitAdmin.declareBinding(binding);

        //创建消息监听器
        SimpleMessageListenerContainer container = containerFactory.createListenerContainer();

        container.setQueues(queue);
        container.setMessageListener(this::handleMessage);
        container.setDefaultRequeueRejected(false);
        container.start();

        messageListenerContainerList.add(container);
    }


    /**
     * 检测是否是主节点
     *
     * @return 是否
     */
    private boolean isLeader() {
        if (!isCluster) {
            return true;
        }

        // TODO: 2021/10/28 以后兼容多节点模式

        return false;
    }


    /**
     * 处理消息
     *
     * @param message       消息
     */
    private void handleMessage(Message message) {
        byte[] body = message.getBody();
        log.info("- receive message");

        String json = new String(body, StandardCharsets.UTF_8);

        syncDataService.syncDelta(json);
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
     * 销毁消息监听器和队列
     */
    @PreDestroy
    public void stopListenerList() {
        messageListenerContainerList.forEach(Lifecycle::stop);

        //如果是随机队列，则删除
        if (!configQueue) {
            rabbitAdmin.deleteQueue(queueName);
            log.info("- stop queue: {}", queueName);
        }

        log.info("- stop all message listeners...");
    }
}
