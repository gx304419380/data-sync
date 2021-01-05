package com.fly.data.sync.config;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fly.data.sync.annotation.SyncIgnore;
import com.fly.data.sync.annotation.SyncTable;
import com.fly.data.sync.entity.DataModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.util.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fly.data.sync.util.StringConverter.LOWER_CAMEL_UNDERSCORE;
import static com.fly.data.sync.util.StringConverter.UPPER_CAMEL_UNDERSCORE;

@Configuration
@Slf4j
public class SyncDataConfig implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Value("${data.sync.scan.package:}")
    private String scanPackage;

    private static final Map<String, DataModel<?>> MODEL_MAP = new ConcurrentHashMap<>();

    private static final List<String> TABLE_LIST = new ArrayList<>();

    public static final CountDownLatch INIT_LATCH = new CountDownLatch(1);

    @SuppressWarnings("unchecked")
    public static <T> DataModel<T> getDataModel(String tableName) {
        waitForInit();
        DataModel<?> dataModel = MODEL_MAP.get(tableName);
        return (DataModel<T>) dataModel;
    }


    public static Map<String, DataModel<?>> getModelMap() {
        waitForInit();
        return MODEL_MAP;
    }

    public static List<String> getTableList() {
        waitForInit();
        return TABLE_LIST;
    }

    /**
     * 等待初始化完成
     */
    private static void waitForInit() {
        try {
            INIT_LATCH.await();
        } catch (InterruptedException e) {
            log.error("- get model error:", e);
        }
    }


    /**
     * 初始化
     * 1.扫描实体类
     * 2.解析并获取表名称、字段
     * 3.包装成数据模型存入缓存
     */
    @PostConstruct
    public void init() {

        if (StringUtils.isEmpty(scanPackage)) {
            scanPackage = getDefaultPackage();
            log.warn("- data.sync.scan.package is empty, use default value: {}", scanPackage);
        }

        List<String> tableClassNameList = scanSyncTableClass();

        if (CollectionUtils.isEmpty(tableClassNameList)) {
            log.warn("- table class list is empty, return...");
            return;
        }

        resolveDataModel(tableClassNameList);

        INIT_LATCH.countDown();
    }



    /**
     * 获取默认包（启动类所在的包）
     * @return default package
     */
    private String getDefaultPackage() {

        return applicationContext.getBeansWithAnnotation(SpringBootApplication.class)
                .values()
                .stream()
                .findFirst()
                .map(Object::getClass)
                .map(Class::getPackage)
                .map(Package::getName)
                .orElseThrow(RuntimeException::new);
    }


    /**
     * 查找所有需要同步的class
     */
    private List<String> scanSyncTableClass() {

        log.info("- begin to scan sync table class in package: {}", scanPackage);

        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false); // 不使用默认的TypeFilter
        provider.addIncludeFilter(new AnnotationTypeFilter(SyncTable.class));
        Set<BeanDefinition> beanDefinitionSet = provider.findCandidateComponents(scanPackage);


        return beanDefinitionSet.stream()
                .map(BeanDefinition::getBeanClassName)
                .collect(Collectors.toList());
    }


    /**
     * 解析数据模型
     *
     * @param tableClassNameList    要同步的数据类名list
     */
    private void resolveDataModel(List<String> tableClassNameList) {

        log.info("- begin to resolve tableClassList: {}", tableClassNameList);

        tableClassNameList.forEach(this::createDataModel);
    }


    /**
     * 创建数据模型
     *
     * @param className     类名称
     */
    @SuppressWarnings("unchecked")
    private <T> void createDataModel(String className) {
        log.info("- begin to create data model for class: {}", className);

        Class<T> modelClass;
        try {
            modelClass = (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("- cannot find class by name: {}", className);
            throw new RuntimeException(e);
        }

        String tableName = resolveTableName(modelClass);

        Field[] fields = modelClass.getDeclaredFields();

        String idName = Stream.of(fields)
                .filter(f -> f.isAnnotationPresent(TableId.class))
                .map(this::resolveTableField)
                .findFirst()
                .orElse(null);

        List<String> fieldNameList = Stream.of(fields)
                .map(this::resolveTableField)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());

        DataModel<T> model = new DataModel<>();
        model.setModelClass(modelClass)
                .setTable(tableName)
                .setId(idName)
                .setFieldList(Arrays.asList(fields))
                .setFieldNameList(fieldNameList);

        MODEL_MAP.put(tableName, model);
        TABLE_LIST.add(tableName);

        log.info("- data model is: {}", model);
    }



    /**
     * 解析表名称
     *
     * @param modelClass    模型类
     * @return              表名称
     */
    private <T> String resolveTableName(Class<T> modelClass) {

        String tableName = null;

        // mybatis plus annotation is prior
        if (modelClass.isAnnotationPresent(TableName.class)) {
            TableName tableNameAnnotation = modelClass.getAnnotation(TableName.class);
            tableName = tableNameAnnotation.value();
        }
        // spring data's annotation is checked then
        else if (modelClass.isAnnotationPresent(Table.class)) {
            Table tableAnnotation = modelClass.getAnnotation(Table.class);
            tableName = tableAnnotation.value();
        }

        if (StringUtils.isNotEmpty(tableName)) {
            return tableName;
        }

        // if there isn't any annotation on the class, use class name as default tableName
        SyncTable syncTable = modelClass.getAnnotation(SyncTable.class);
        tableName = syncTable.value();

        if (StringUtils.isNotEmpty(tableName)) {
            return tableName;
        }

        return UPPER_CAMEL_UNDERSCORE.convert(modelClass.getSimpleName());
    }


    /**
     * 解析字段名称
     *
     * @param field     模型字段
     * @return          对应的表字段
     */
    private String resolveTableField(Field field) {

        if (field.isAnnotationPresent(SyncIgnore.class)) {
            return null;
        }

        String fieldName = null;

        //mybatis-plus注解优先
        if (field.isAnnotationPresent(TableField.class)) {
            TableField tableField = field.getAnnotation(TableField.class);
            fieldName = tableField.value();
        }
        //mybatis-plus注解TableId
        else if (field.isAnnotationPresent(TableId.class)) {
            TableId tableId = field.getAnnotation(TableId.class);
            fieldName = tableId.value();
        }
        //spring data注解
        else if (field.isAnnotationPresent(Column.class)) {
            Column column = field.getAnnotation(Column.class);
            fieldName = column.value();
        }

        if (StringUtils.isNotEmpty(fieldName)) {
            return fieldName;
        }

        return LOWER_CAMEL_UNDERSCORE.convert(field.getName());
    }


    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}