package com.fly.data.sync.entity;

import lombok.Data;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
@Data
public class SyncMessage<T> {

    private String table;
    /**
     * ADD DELETE UPDATE
     */
    private String type;

    /**
     * changed data's id list
     */
    private List<Object> idList;

    private List<T> data;
}
