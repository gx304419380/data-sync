package com.fly.data.sync.entity;

import lombok.Data;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/27
 */
@Data
public class ResponseDto<T> {
    private Integer code;
    private String msg;
    private T data;
}
