package com.fly.data.sync.event;

import com.fly.data.sync.constant.SyncEventSource;
import lombok.Data;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/6
 */
@Data
public class SyncAllEvent {

    private SyncEventSource source;

    public SyncAllEvent(SyncEventSource source) {
        this.source = source;
    }
}
