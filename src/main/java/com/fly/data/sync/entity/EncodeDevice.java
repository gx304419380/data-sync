package com.fly.data.sync.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fly.data.sync.annotation.SyncTable;
import lombok.Data;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/6
 */
@TableName("encode_device")
@SyncTable
@Data
public class EncodeDevice {

    @TableId("device_id")
    private String id;
    private String type;

    @TableField("device_ip")
    private String ip;

}
