package com.fly.data.sync.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

import static com.fly.data.sync.util.SyncCheck.notEmpty;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaveOrUpdateResult<T> {
    private List<T> addList;
    private UpdateData<T> updateData;

    public boolean hasAddList() {
        return notEmpty(addList);
    }

    public boolean hasUpdateData() {
        return updateData != null && updateData.isNotEmpty();
    }
}
