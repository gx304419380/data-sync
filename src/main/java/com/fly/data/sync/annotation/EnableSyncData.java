package com.fly.data.sync.annotation;

import com.fly.data.sync.config.SyncDataConfig;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/10/22
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SyncDataConfig.class)
public @interface EnableSyncData {
}
