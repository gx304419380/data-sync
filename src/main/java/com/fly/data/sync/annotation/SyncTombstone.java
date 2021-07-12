package com.fly.data.sync.annotation;

import java.lang.annotation.*;

/**
 * 逻辑删除字段注解
 *
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/11
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SyncTombstone {
    /**
     * 删除值
     */
    String deleteValue() default "1";

    /**
     * 未删除值
     */
    String existValue() default "0";
}
