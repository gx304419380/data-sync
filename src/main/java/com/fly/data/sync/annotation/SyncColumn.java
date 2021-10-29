package com.fly.data.sync.annotation;

import java.lang.annotation.*;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SyncColumn {

    /**
     * @return column name
     */
    String value() default "";
}
