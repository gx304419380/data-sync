package com.fly.data.sync.util;

import lombok.experimental.UtilityClass;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/8/25
 */
@UtilityClass
public class SyncCheck {


    /**
     * 是否为null
     * @param obj obj
     * @return  是否为空
     */
    public static boolean isNull(Object obj) {
        return obj == null;
    }


    /**
     * 是否为null
     * @param obj obj
     * @return  是否为空
     */
    public static boolean notNull(Object obj) {
        return obj != null;
    }


    /**
     * 校验相等
     * @param a     a
     * @param b     b
     * @return      相等
     */
    public static boolean isEqual(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }


    /**
     * 校验相等
     * @param a     a
     * @param b     b
     * @return      相等
     */
    public static boolean notEqual(Object a, Object b) {
        return !isEqual(a, b);
    }


    /**
     * Determine whether the given array is not empty:
     * @param array the array to check
     */
    public static boolean notEmpty(Object[] array) {
        return (array != null && array.length > 0);
    }


    /**
     * Determine whether the given array is empty:
     * i.e. {@code null} or of zero length.
     * @param array the array to check
     */
    public static boolean isEmpty(Object[] array) {
        return (array == null || array.length == 0);
    }

    /**
     * 是否为空 String
     * @param obj obj
     * @return      true false
     */
    public static boolean isEmpty(String obj) {
        if (obj == null) {
            return true;
        }
        return obj.length() == 0;
    }

    /**
     * 是否为空 String
     * @param obj obj
     * @return      true false
     */
    public static boolean notEmpty(String obj) {
        if (obj == null) {
            return false;
        }
        return obj.length() > 0;
    }

    /**
     * 是否为空 Collection
     * @param obj obj
     * @return      true false
     */
    public static boolean isEmpty(Collection<?> obj) {
        if (obj == null) {
            return true;
        }
        return obj.isEmpty();
    }

    /**
     * 是否为空 Collection
     * @param obj obj
     * @return      true false
     */
    public static boolean notEmpty(Collection<?> obj) {
        if (obj == null) {
            return false;
        }
        return !obj.isEmpty();
    }

    /**
     * 是否为空 Map
     * @param obj obj
     * @return      true false
     */
    public static boolean isEmpty(Map<?, ?> obj) {
        if (obj == null) {
            return true;
        }
        return obj.isEmpty();
    }

    /**
     * 是否为空 Map
     * @param obj obj
     * @return      true false
     */
    public static boolean notEmpty(Map<?, ?> obj) {
        if (obj == null) {
            return false;
        }
        return !obj.isEmpty();
    }


    /**
     * 是否为空
     * @param obj obj
     * @return      true false
     */
    public static boolean isEmpty(Object obj) {
        if (obj == null) {
            return true;
        }

        if (obj instanceof CharSequence) {
            return ((CharSequence) obj).length() == 0;
        }
        if (obj instanceof Collection) {
            return ((Collection<?>) obj).isEmpty();
        }
        if (obj instanceof Map) {
            return ((Map<?, ?>) obj).isEmpty();
        }
        if (obj.getClass().isArray()) {
            return Array.getLength(obj) == 0;
        }
        if (obj instanceof Optional) {
            return !((Optional<?>) obj).isPresent();
        }

        // else
        return false;
    }

    /**
     * 是否为空
     * @param obj obj
     * @return      true false
     */
    public static boolean notEmpty(Object obj) {
        return !isEmpty(obj);
    }


    /**
     * 是否为空 String
     * @param cs cs
     * @return      true false
     */
    public static boolean isBlank(CharSequence cs) {
        int strLen = cs == null ? 0 : cs.length();
        if (strLen == 0) {
            return true;
        }

        for(int i = 0; i < strLen; ++i) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }

        return true;
    }


    /**
     * 是否为空 String
     * @param cs    cs
     * @return      true false
     */
    public static boolean notBlank(CharSequence cs) {
        return !isBlank(cs);
    }

}
