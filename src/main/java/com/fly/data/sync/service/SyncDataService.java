package com.fly.data.sync.service;

import com.fly.data.sync.entity.DataModel;

public interface SyncDataService {

    <T> void syncTotal(DataModel<T> model);

    <T> void syncDelta(DataModel<T> model, String message);
}
