package com.fly.data.sync.util;

import com.fasterxml.jackson.databind.PropertyNamingStrategy.SnakeCaseStrategy;
import lombok.experimental.UtilityClass;

/**
 * @author  guoxiang
 * @version 1.0
 * @since   20210101
 */
@UtilityClass
public class StringConverter {

    private static final SnakeCaseStrategy SNAKE_CASE_STRATEGY = new SnakeCaseStrategy();

    /**
     * 驼峰转下划线
     */
    public static String toUnderscore(String value) {
        return SNAKE_CASE_STRATEGY.translate(value);
    }

}
