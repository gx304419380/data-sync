package com.fly.data.sync.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.fly.data.sync.annotation.SyncIgnore;
import com.fly.data.sync.annotation.SyncTable;
import lombok.Data;
import org.springframework.data.relational.core.mapping.Table;

@SyncTable
@Data
@Table
public class Device {
    @TableId("device_id")
    private String id;
    private String type;
    private String ip;
    @SyncIgnore
    private Integer testIgnore;

    @TableField("test_anno")
    private String testAnno;

}
