package com.fly.data.sync.constant;

/**
 * 同步任务事件来源
 */
public enum SyncEventSource {
    //项目启动
    APPLICATION_START,
    //定时任务
    SCHEDULE_JOB,
    //监听MQ
    MESSAGE_QUEUE,
    //其他
    OTHER
}
