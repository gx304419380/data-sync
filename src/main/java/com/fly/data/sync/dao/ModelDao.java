package com.fly.data.sync.dao;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.SaveOrUpdateResult;
import com.fly.data.sync.entity.UpdateData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;

import static com.fly.data.sync.util.SyncCheck.isEmpty;
import static java.util.stream.Collectors.*;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
public class ModelDao {

    private final JdbcTemplate jdbcTemplate;

    private final NamedParameterJdbcTemplate namedJdbcTemplate;

    public ModelDao(JdbcTemplate jdbcTemplate, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedJdbcTemplate = namedParameterJdbcTemplate;
    }

    
    public <T> void loadToTemp(List<T> dataList, DataModel<T> model) {

        String insertSql = model.getInsertTempSql();

        namedJdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(dataList));

        if (!model.isTombstone()) {
            return;
        }

        String tombstone = model.getTombstoneColumn();
        String table = model.getTable();
        jdbcTemplate.update("update " + table + " set " + tombstone + "=" + model.getNotDeletedValue());
    }

    
    public <T> List<T> add(DataModel<T> model) {

        String queryAddSql = model.getQueryAddSql();
        String addSql = model.getAddSql();

        List<T> addList = jdbcTemplate.query(queryAddSql, model.getRowMapper());
        if (ObjectUtils.isEmpty(addList)) {
            return Collections.emptyList();
        }

        jdbcTemplate.update(addSql);

        return addList;
    }


    
    public <T> UpdateData<T> update(DataModel<T> model) {

        String queryUpdateSql = model.getQueryUpdateSql();
        String queryOldSql = model.getQueryOldSql();
        String updateSql = model.getUpdateAllSql();

        List<T> updateList = jdbcTemplate.query(queryUpdateSql, model.getRowMapper());
        if (ObjectUtils.isEmpty(updateList)) {
            return UpdateData.empty();
        }

        List<T> oldList = jdbcTemplate.query(queryOldSql, model.getRowMapper());
        jdbcTemplate.update(updateSql);

        return new UpdateData<>(updateList, oldList);
    }


    
    public <T> List<T> delete(DataModel<T> model) {

        String queryDeleteSql = model.getQueryDeleteSql();

        String deleteSql = model.getDeleteAllSql();

        List<T> deleteList = jdbcTemplate.query(queryDeleteSql, model.getRowMapper());
        if (ObjectUtils.isEmpty(deleteList)) {
            return Collections.emptyList();
        }

        jdbcTemplate.update(deleteSql);

        return deleteList;
    }


    
    public <T> void deleteTemp(DataModel<T> model) {
        jdbcTemplate.execute("delete from " + model.getTempTable());
    }


    
    public void createTempTableIfNotExist(String table) {
        String sql = "create table if not exists " + table +"_temp like " + table;
        jdbcTemplate.execute(sql);
    }

    /**
     * 根据id批量查询
     *
     * @param model     模型
     * @param idList    id list
     * @param <T>       泛型
     * @return          list
     */
    public <T> List<T> getListById(DataModel<T> model, Collection<Object> idList) {
        if (isEmpty(idList)) {
            return Collections.emptyList();
        }

        Map<String, Object> params = Collections.singletonMap("idList", idList);

        String sql = "select * from " + model.getTable() + " where " + model.getIdColumn() + " in (:idList)";

        return namedJdbcTemplate.query(sql, params, model.getRowMapper());
    }

    /**
     * 增量增加
     * @param model     模型
     * @param idList    id列表
     * @param data      数据
     * @param <T>       泛型
     */
    public <T> SaveOrUpdateResult<T> saveOrUpdateDelta(DataModel<T> model, List<Object> idList, List<T> data) {
        if (isEmpty(data)) {
            return new SaveOrUpdateResult<>();
        }

        List<T> existList = getListById(model, idList);
        Set<Object> existSet = existList.stream().map(model::getIdOf).collect(toSet());

        Map<Boolean, List<T>> map = data.stream().collect(groupingBy(d -> existSet.contains(model.getIdOf(d))));

        List<T> updateList = map.get(Boolean.TRUE);
        List<T> addList = map.get(Boolean.FALSE);

        insertDelta(model, addList);
        UpdateData<T> updateData = updateDelta(model, existSet, updateList);

        return new SaveOrUpdateResult<>(addList, updateData);
    }


    /**
     * 批量插入
     *
     * @param model 模型
     * @param data  数据
     * @param <T>   泛型
     */
    public <T> void insertDelta(DataModel<T> model, List<T> data) {
        if (isEmpty(data)) {
            return;
        }

        String insertSql = model.getInsertSql();
        SqlParameterSource[] batch = SqlParameterSourceUtils.createBatch(data);
        namedJdbcTemplate.batchUpdate(insertSql, batch);
    }

    /**
     * 根据id list删除
     *
     * @param model     模型
     * @param idList    id list
     * @param <T>       泛型
     * @return          被删除的数据
     */
    public <T> List<T> deleteDelta(DataModel<T> model, List<Object> idList) {
        List<T> data = getListById(model, idList);

        List<Object[]> paramList = idList.stream().map(id -> new Object[]{id}).collect(toList());
        jdbcTemplate.batchUpdate(model.getDeleteDeltaSql(), paramList);
        return data;
    }

    public <T> UpdateData<T> updateDelta(DataModel<T> model, Collection<Object> idList, List<T> data) {
        if (isEmpty(data)) {
            return UpdateData.empty();
        }

        List<T> oldData = getListById(model, idList);

        String updateDeltaSql = model.getUpdateDeltaSql();
        namedJdbcTemplate.batchUpdate(updateDeltaSql, SqlParameterSourceUtils.createBatch(data));

        return new UpdateData<>(oldData, data);
    }
}
