package com.fly.data.sync.service;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.PageDto;
import com.fly.data.sync.entity.ResponseDto;
import com.fly.data.sync.util.SyncJsonUtils;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class DefaultEtlServiceImpl implements EtlService {

    private final RestTemplate restTemplate;

    @Value("${sync.data.url:http://bapp-mes-upms-biz/sync?table={1}&pageNo={2}&pageSize={3}}")
    private String url;

    @Override
    public <T> PageDto<T> page(DataModel<T> model, int page, int size) {

        ParameterizedTypeReference<ResponseDto<T>> type =
                SyncJsonUtils.getJavaType(ResponseDto.class, model.getModelClass());

        ResponseEntity<ResponseDto<T>> responseEntity =
                restTemplate.exchange(url, HttpMethod.GET, null, type, model.getTable(), page, size);

        ResponseDto<T> body = responseEntity.getBody();
        Assert.notNull(body, "response is null" + responseEntity);
        Assert.isTrue(body.getCode() == 0, "response error:" + body.getMsg());

        return body.getData();
    }
}
