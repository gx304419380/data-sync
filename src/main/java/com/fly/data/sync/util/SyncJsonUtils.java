package com.fly.data.sync.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalTimeSerializer;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.springframework.core.ParameterizedTypeReference;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.fly.data.sync.util.SyncCheck.isEmpty;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
@UtilityClass
public class SyncJsonUtils {
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String TIME_PATTERN = "HH:mm:ss";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        // 初始化JavaTimeModule
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        //处理LocalDateTime
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN);
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(dateTimeFormatter));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(dateTimeFormatter));

        //处理LocalDate
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
        javaTimeModule.addSerializer(LocalDate.class, new LocalDateSerializer(dateFormatter));
        javaTimeModule.addDeserializer(LocalDate.class, new LocalDateDeserializer(dateFormatter));

        //处理LocalTime
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(TIME_PATTERN);
        javaTimeModule.addSerializer(LocalTime.class, new LocalTimeSerializer(timeFormatter));
        javaTimeModule.addDeserializer(LocalTime.class, new LocalTimeDeserializer(timeFormatter));

        //注册时间模块, 支持支持jsr310, 即新的时间类(java.time包下的时间类)
        MAPPER.registerModule(javaTimeModule);
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
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

    public static <T> ParameterizedTypeReference<T> getJavaType(Class<?> parametrized, Class<?>... parameterClasses) {
        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(parametrized, parameterClasses);
        return ParameterizedTypeReference.forType(javaType);
    }

    @SneakyThrows
    public static <T> List<T> toList(List<Object> list, Class<T> modelClass) {
        if (isEmpty(list)) {
            return Collections.emptyList();
        }

        return list.stream().map(m -> MAPPER.convertValue(m, modelClass)).collect(Collectors.toList());
    }
}
