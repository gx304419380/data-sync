package com.fly.data.sync.entity;

import lombok.Data;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
@Data
public class UpdateData<T> {
    private List<T> data;
    private List<T> oldData;

    public UpdateData(List<T> data, List<T> oldData) {
        this.data = data;
        this.oldData = oldData;
    }

    public Boolean isNotEmpty() {
        return data != null && !data.isEmpty();
    }

    public int size() {
        return data.size();
    }
}
