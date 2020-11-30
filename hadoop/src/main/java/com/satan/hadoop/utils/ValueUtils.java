package com.satan.hadoop.utils;

import com.satan.hadoop.annotion.Value;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2020/11/15
 */
public final class ValueUtils {

    public static void getValue(Object obj) throws Exception {
        Class<?> objClass = obj.getClass();
        Field[] fields = objClass.getDeclaredFields();
        for (Field f : fields) {
            boolean annotationPresent = f.isAnnotationPresent(Value.class);
            if (annotationPresent) {
                Value valueAnnotation = f.getAnnotation(Value.class);
                String value = valueAnnotation.value();
                String proValue = PropertiesUtils.getValue(value);
                Objects.requireNonNull(proValue);
                // 获取属性的名称
                String name = f.getName();
                // 首字母大写,拼接set方法
                name = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
                // 属性的类型
                String nameType = f.getGenericType().getTypeName();
                switch (nameType) {
                    // 包装类
                    case "java.lang.String": {
                        Method method = objClass.getMethod(name, String.class);
                        method.invoke(obj, proValue);
                        break;
                    }
                    case "java.lang.Integer": {
                        Method method = objClass.getMethod(name, Integer.class);
                        method.invoke(obj, Integer.parseInt(proValue));
                        break;
                    }
                    case "java.lang.Float": {
                        Method method = objClass.getMethod(name, Float.class);
                        method.invoke(obj, Float.parseFloat(proValue));
                        break;
                    }
                    case "java.lang.Double": {
                        Method method = objClass.getMethod(name, Double.class);
                        method.invoke(obj, Double.parseDouble(proValue));
                        break;
                    }
                    case "java.lang.Long": {
                        Method method = objClass.getMethod(name, Long.class);
                        method.invoke(obj, Long.parseLong(proValue));
                        break;
                    }
                    case "java.lang.Boolean": {
                        Method method = objClass.getMethod(name, Boolean.class);
                        if ("true".equals(proValue)) {
                            method.invoke(obj, Boolean.TRUE);
                        } else {
                            method.invoke(obj, Boolean.FALSE);
                        }
                        break;
                    }
                    // 基本类型
                    case "int": {
                        Method method = objClass.getMethod(name, int.class);
                        method.invoke(obj, Integer.parseInt(proValue));
                        break;
                    }
                    case "float": {
                        Method method = objClass.getMethod(name, float.class);
                        method.invoke(obj, Float.parseFloat(proValue));
                        break;
                    }
                    case "double": {
                        Method method = objClass.getMethod(name, double.class);
                        method.invoke(obj, Double.parseDouble(proValue));
                        break;
                    }
                    case "long": {
                        Method method = objClass.getMethod(name, long.class);
                        method.invoke(obj, Long.parseLong(proValue));
                        break;
                    }
                    case "boolean": {
                        Method method = objClass.getMethod(name, boolean.class);
                        method.invoke(obj, "true".equals(proValue));
                        break;
                    }

                    // ... 时间类型需要特殊转换
                    default:
                }

            }
        }
    }
}
