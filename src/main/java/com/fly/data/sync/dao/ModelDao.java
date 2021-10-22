package com.fly.data.sync.dao;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.util.ObjectUtils;

import java.util.Collections;
import java.util.List;

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

        String insertSql = model.getInsertSql();

        namedJdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(dataList));

        if (!model.isTombstone()) {
            return;
        }

        String tombstone = model.getTombstoneField();
        String table = model.getTable();
        jdbcTemplate.update("update " + table + " set " + tombstone + "=0");
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
        String updateSql = model.getUpdateSql();

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

        String deleteSql = model.getDeleteSql();

        List<T> deleteList = jdbcTemplate.query(queryDeleteSql, model.getRowMapper());
        if (ObjectUtils.isEmpty(deleteList)) {
            return Collections.emptyList();
        }

        jdbcTemplate.update(deleteSql);

        return deleteList;
    }


    
    public <T> void deleteTemp(DataModel<T> model) {
        jdbcTemplate.execute("delete * from " + model.getTempTable());
    }


    
    public void createTempTableIfNotExist(String table) {
        String sql = "create table if not exists " + table +"_temp like " + table;
        jdbcTemplate.execute(sql);
    }
}
