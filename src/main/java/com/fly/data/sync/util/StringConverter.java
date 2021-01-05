package com.fly.data.sync.util;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;

public class StringConverter {
    //大写驼峰转下划线
    public static Converter<String, String> UPPER_CAMEL_UNDERSCORE =
            CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE);

    //小写驼峰转下划线
    public static Converter<String, String> LOWER_CAMEL_UNDERSCORE =
            CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE);

}
