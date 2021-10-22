package com.fly.data.sync.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.data.sync.entity.SyncMessage;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.springframework.core.ParameterizedTypeReference;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
@UtilityClass
public class SyncJsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }


    @SneakyThrows
    public static <T> T toBean(String json, Class<T> type) {
        return MAPPER.readValue(json, type);
    }

    @SneakyThrows
    public static <T> T toBean(String json, Class<T> outerType, Class<?> innerType) {
        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(outerType, innerType);
        return MAPPER.readValue(json, javaType);
    }

    @SneakyThrows
    public static <T> SyncMessage<T> toSyncMessage(String json, Class<T> type) {
        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(SyncMessage.class, type);
        return MAPPER.readValue(json, javaType);
    }

    public static <T> ParameterizedTypeReference<T> getJavaType(Class<?> parametrized, Class<?>... parameterClasses) {
        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(parametrized, parameterClasses);
        return ParameterizedTypeReference.forType(javaType);
    }
}
