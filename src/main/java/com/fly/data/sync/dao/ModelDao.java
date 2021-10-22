package com.fly.data.sync.dao;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.util.ObjectUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.fly.data.sync.util.SyncCheck.isEmpty;

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

    public <T> void addDelta(DataModel<T> model, List<T> data) {
        String insertSql = model.getInsertSql();
        SqlParameterSource[] batch = SqlParameterSourceUtils.createBatch(data);
        namedJdbcTemplate.batchUpdate(insertSql, batch);
    }

    public <T> List<T> deleteDelta(DataModel<T> model, List<Object> idList) {
        if (isEmpty(idList)) {
            return Collections.emptyList();
        }

        Map<String, Object> params = Collections.singletonMap("idList", idList);

        String deleteSql = model.getDeleteDeltaSql();
        String sql = "select * from " + model.getTable() + " where " + model.getIdColumn() + " in (:idList)";

        List<T> data = namedJdbcTemplate.query(sql, params, model.getRowMapper());

        List<Object[]> paramList = idList.stream().map(id -> new Object[]{id}).collect(Collectors.toList());
        jdbcTemplate.batchUpdate(deleteSql, paramList);
        return data;
    }

    public <T> UpdateData<T> updateDelta(DataModel<T> model, List<Object> idList, List<T> data) {
        if (isEmpty(data)) {
            return UpdateData.empty();
        }

        String sql = "select * from " + model.getTable() + " where " + model.getIdColumn() + " in (:idList)";

        Map<String, Object> params = Collections.singletonMap("idList", idList);
        List<T> oldData = namedJdbcTemplate.query(sql, params, model.getRowMapper());

        String updateDeltaSql = model.getUpdateDeltaSql();
        namedJdbcTemplate.batchUpdate(updateDeltaSql, SqlParameterSourceUtils.createBatch(data));

        return new UpdateData<>(oldData, data);
    }
}
