package com.fly.data.sync.service;

import com.fly.data.sync.entity.DataModel;
import org.springframework.amqp.core.Message;

public interface SyncDataService {

    <T> void syncAll(DataModel<T> model);

    <T> void syncDelta(DataModel<T> model, Message message);
}
