package com.fly.data.sync.entity;

import lombok.Data;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
@Data
public class PageDto<T> {

    private List<T> data;

    private Long total;

}
