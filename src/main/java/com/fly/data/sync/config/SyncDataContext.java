package com.fly.data.sync.config;

import com.fly.data.sync.annotation.SyncTable;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.util.SyncCheck;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
@Slf4j
public class SyncDataContext implements ApplicationContextAware {
    private ApplicationContext applicationContext;

    @Value("${data.sync.scan.package:}")
    private String scanPackage;

    private static final Map<String, DataModel<?>> MODEL_MAP = new ConcurrentHashMap<>();

    private static final List<String> TABLE_LIST = new ArrayList<>();


    /**
     * 初始化
     * 1.扫描实体类
     * 2.解析并获取表名称、字段
     * 3.包装成数据模型存入缓存
     */
    @PostConstruct
    public void init() {

        if (SyncCheck.isEmpty(scanPackage)) {
            scanPackage = getDefaultPackage();
            log.warn("- data.sync.scan.package is empty, use default value: {}", scanPackage);
        }

        List<String> tableClassNameList = scanSyncTableClass();

        if (CollectionUtils.isEmpty(tableClassNameList)) {
            log.warn("- table class list is empty, return...");
            return;
        }

        resolveDataModel(tableClassNameList);
    }



    @SuppressWarnings("unchecked")
    public <T> DataModel<T> getDataModel(String tableName) {
        DataModel<?> dataModel = MODEL_MAP.get(tableName);
        return (DataModel<T>) dataModel;
    }

    public List<String> getTableList() {
        return TABLE_LIST;
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

        // 不使用默认的TypeFilter
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
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

        DataModel<T> model = new DataModel<>(modelClass);

        MODEL_MAP.put(model.getTable(), model);
        TABLE_LIST.add(model.getTable());

        log.info("- data model is: {}", model);
    }



    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
