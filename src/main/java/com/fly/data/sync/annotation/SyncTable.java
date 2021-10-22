package com.fly.data.sync.annotation;

import java.lang.annotation.*;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface SyncTable {

    /**
     * @return table name
     */
    String value() default "";

    /**
     * @return queue name
     */
    String queue() default "";
}
