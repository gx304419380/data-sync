package com.fly.data.sync.entity;

import com.fly.data.sync.annotation.SyncTable;
import lombok.Data;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@SyncTable
@Data
@Table("tb_device")
public class Device {
    private Long id;
    private String type;
    private String ip;
    private Integer port;
    private String name;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

}
