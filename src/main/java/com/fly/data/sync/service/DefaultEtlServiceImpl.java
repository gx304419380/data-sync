package com.fly.data.sync.service;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.PageDto;
import com.fly.data.sync.util.SyncJsonUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

/**
 * default implementation
 *
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
public class DefaultEtlServiceImpl implements EtlService {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${sync.data.url:}")
    private String url;

    @Override
    public <T> PageDto<T> page(DataModel<T> model, int page, int size) {

        ParameterizedTypeReference<PageDto<T>> type = SyncJsonUtils.getJavaType(PageDto.class, model.getModelClass());

        ResponseEntity<PageDto<T>> responseEntity =
                restTemplate.exchange(url, HttpMethod.GET, null, type, page, size);

        PageDto<T> body = responseEntity.getBody();
        Assert.notNull(body, "response is null" + responseEntity);

        return body;
    }
}
