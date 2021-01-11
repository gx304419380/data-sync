package com.fly.data.sync.dao.impl;

import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
@Repository
public class ModelDaoImpl implements ModelDao {

    private final JdbcTemplate jdbcTemplate;

    private final NamedParameterJdbcTemplate namedJdbcTemplate;

    public ModelDaoImpl(JdbcTemplate jdbcTemplate, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedJdbcTemplate = namedParameterJdbcTemplate;
    }

    @Override
    public <T> void loadToTemp(List<T> dataList, DataModel<T> model) {

        String insertSql = model.getInsertSql();

        namedJdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(dataList));
    }

    @Override
    public <T> List<T> add(DataModel<T> model) {

        String queryAddSql = model.getQueryAddSql();
        String addSql = model.getAddSql();

        List<T> addList = jdbcTemplate.query(queryAddSql, model.getRowMapper());
        if (CollectionUtils.isEmpty(addList)) {
            return Collections.emptyList();
        }

        jdbcTemplate.update(addSql);

        return addList;
    }


    @Override
    public <T> UpdateData<T> update(DataModel<T> model) {

        String queryUpdateSql = model.getQueryUpdateSql();
        String queryOldSql = model.getQueryOldSql();
        String updateSql = model.getUpdateSql();

        List<T> updateList = jdbcTemplate.query(queryUpdateSql, model.getRowMapper());
        if (CollectionUtils.isEmpty(updateList)) {
            return UpdateData.empty();
        }

        List<T> oldList = jdbcTemplate.query(queryOldSql, model.getRowMapper());
        jdbcTemplate.update(updateSql);

        return new UpdateData<>(updateList, oldList);
    }


    @Override
    public <T> List<T> delete(DataModel<T> model) {

        String queryDeleteSql = model.getQueryDeleteSql();

        String deleteSql = model.getDeleteSql();

        List<T> deleteList = jdbcTemplate.query(queryDeleteSql, model.getRowMapper());
        if (CollectionUtils.isEmpty(deleteList)) {
            return Collections.emptyList();
        }

        jdbcTemplate.update(deleteSql);

        return deleteList;
    }


    @Override
    public <T> void truncateTemp(DataModel<T> model) {
        jdbcTemplate.execute("truncate " + model.getTempTable());
    }


    @Override
    public void createTempTableIfNotExist(String table) {
        String sql = "create table if not exists " + table +"_temp like " + table;
        jdbcTemplate.execute(sql);
    }
}
