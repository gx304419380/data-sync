package com.fly.data.sync.util;

import com.google.common.base.Converter;

import static com.google.common.base.CaseFormat.*;

/**
 * @author  guoxiang
 * @version 1.0
 * @since   20210101
 */
public class StringConverter {

    private StringConverter() {
    }

    //大写驼峰转下划线
    public static Converter<String, String> UPPER_CAMEL_UNDERSCORE =
            UPPER_CAMEL.converterTo(LOWER_UNDERSCORE);

    //小写驼峰转下划线
    public static Converter<String, String> LOWER_CAMEL_UNDERSCORE =
            LOWER_CAMEL.converterTo(LOWER_UNDERSCORE);

}
