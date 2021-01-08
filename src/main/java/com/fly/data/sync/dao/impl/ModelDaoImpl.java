package com.fly.data.sync.dao.impl;

import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Repository;

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

        String insertSql = "insert into ${tempTable} (${fieldList}) values (${propertyList})";

        insertSql = parseSql(insertSql, model);

        namedJdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(dataList));
    }

    @Override
    public <T> List<T> add(DataModel<T> model) {
        //b差a
        String queryAddSql = "select ${a.fieldList} from ${tempTable} a " +
                "left join ${table} b on a.${id} = b.${id} " +
                "where b.${id} is null";

        String addSql = "insert into ${table} (${fieldList}) " +
                "select ${a.fieldList} from ${tempTable} a " +
                "left join ${table} b on a.${id} = b.${id} " +
                "where b.${id} is null";

        queryAddSql = parseSql(queryAddSql, model);
        addSql = parseSql(addSql, model);

        List<T> addList = jdbcTemplate.query(queryAddSql, model.getRowMapper());
        jdbcTemplate.update(addSql);

        return addList;
    }


    @Override
    public <T> UpdateData<T> update(DataModel<T> model) {

        String queryUpdateSql = "select ${a.fieldList} from ${tempTable} a " +
                "left join ${table} b on a.${id} = b.${id} " +
                "where a.${updateTime} > b.${updateTime}";

        String queryOldSql = "select ${b.fieldList} from ${tempTable} a " +
                "left join ${table} b on a.${id} = b.${id} " +
                "where a.${updateTime} > b.${updateTime}";

        String updateSql = "update ${table}, ${tempTable} " +
                "set ${updateField}=${updateField} " +
                "where ${table}.${id} = ${tempTable}.${id} " +
                "and ${table}.${updateTime} < ${tempTable}.${updateTime}";

        queryUpdateSql = parseSql(queryUpdateSql, model);
        queryOldSql = parseSql(queryOldSql, model);
        updateSql = parseSql(updateSql, model);

        List<T> updateList = jdbcTemplate.query(queryUpdateSql, model.getRowMapper());
        List<T> oldList = jdbcTemplate.query(queryOldSql, model.getRowMapper());
        jdbcTemplate.update(updateSql);

        return new UpdateData<>(updateList, oldList);
    }


    @Override
    public <T> List<T> delete(DataModel<T> model) {
        String queryDeleteSql = "select ${a.fieldList} from ${table} a " +
                "left join ${tempTable} b on a.${id} = b.${id} " +
                "where b.${id} is null";

        String deleteSql = "delete a from ${table} a " +
                "left join ${tempTable} b on a.${id} = b.${id} " +
                "where b.${id} is null";


        queryDeleteSql = parseSql(queryDeleteSql, model);

        deleteSql = parseSql(deleteSql, model);

        List<T> deleteList = jdbcTemplate.query(queryDeleteSql, model.getRowMapper());

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

    private <T> String parseSql(String sql, DataModel<T> model) {
        return sql.replace("${id}", model.getId())
                .replace("${table}", model.getTable())
                .replace("${tempTable}", model.getTempTable())
                .replace("${updateTime}", model.getUpdateTime())
                .replace("${fieldList}", model.getFieldNameListString())
                .replace("${propertyList}", model.getPropertyNameString())
                .replace("${a.fieldList}", model.getFieldNameListStringWithPrefix("a"))
                .replace("${b.fieldList}", model.getFieldNameListStringWithPrefix("b"))
                .replace("${updateField}=${updateField}", model.getUpdateFieldString());
    }
}
