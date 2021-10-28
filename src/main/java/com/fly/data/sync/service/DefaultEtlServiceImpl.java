package com.fly.data.sync.service;

import com.fly.data.sync.config.SyncDataContext;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.PageDto;
import com.fly.data.sync.entity.ResponseDto;
import com.fly.data.sync.entity.SyncMessage;
import com.fly.data.sync.util.SyncCheck;
import com.fly.data.sync.util.SyncJsonUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

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

    private final SyncDataContext syncDataContext;

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

    /**
     * @param message json字符串
     * @param <T>   泛型
     * @return      message
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> SyncMessage<T> convertMessage(String message) {
        Map<String, Object> map = SyncJsonUtils.toMap(message);

        String table = (String) map.getOrDefault("table", "");
        String type = (String) map.getOrDefault("type", "");

        List<Object> originalData = (List<Object>) map.getOrDefault("data", emptyList());
        DataModel<T> model = syncDataContext.getDataModel(table);
        Assert.notNull(model, "cannot find model for table: " + table);

        List<T> data = SyncJsonUtils.toList(originalData, model.getModelClass());

        Object idListObject = map.getOrDefault("idList", emptyList());
        List<Object> idList = (List<Object>) idListObject;

        if (SyncCheck.isEmpty(idList)) {
            idList = originalData.stream()
                    .map(Map.class::cast)
                    .map(m -> m.get(model.getIdField()))
                    .collect(toList());
        }

        return new SyncMessage<>(table, type, idList, data);
    }
}
