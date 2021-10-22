package com.fly.data.sync.constant;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
public class SyncConstant {

    private SyncConstant() {
    }

    public static final String UPDATE_TIME_FIELD = "update_time";
    public static final String TOMBSTONE_FIELD = "is_delete";
    public static final String ID_FIELD = "id";
    public static final String TEMP_SUFFIX = "_temp";

    public static final String ADD = "ADD";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
}
