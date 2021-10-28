package com.fly.data.sync.config;

import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.service.DefaultEtlServiceImpl;
import com.fly.data.sync.service.EtlService;
import com.fly.data.sync.service.SyncDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.client.RestTemplate;

/**
 * sync data config
 *
 * @author guoxiang
 */
@Configuration
@Slf4j
@EnableAsync
@EnableRetry
public class SyncDataConfig {

    @Bean
    @ConditionalOnMissingBean(EtlService.class)
    public EtlService etlService(RestTemplate restTemplate) {
        return new DefaultEtlServiceImpl(restTemplate);
    }

    @Bean
    public SyncDataContext syncDataContext() {
        return new SyncDataContext();
    }

    @Bean
    public ModelDao modelDao(JdbcTemplate jdbcTemplate,
                             NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        return new ModelDao(jdbcTemplate, namedParameterJdbcTemplate);
    }

    @Bean
    public SyncDataService syncDataService(ModelDao modelDao,
                                           EtlService etlService,
                                           ApplicationEventPublisher publisher,
                                           SyncDataContext syncDataContext) {
        return new SyncDataService(modelDao, publisher, etlService, syncDataContext);
    }

    @Bean
    public SyncDataListener syncDataListener(ApplicationEventPublisher publisher,
                                             SimpleRabbitListenerContainerFactory containerFactory,
                                             AmqpAdmin rabbitAdmin,
                                             SyncDataService syncDataService,
                                             SyncDataContext syncDataContext) {
        return new SyncDataListener(publisher, containerFactory, rabbitAdmin, syncDataService, syncDataContext);
    }

}
