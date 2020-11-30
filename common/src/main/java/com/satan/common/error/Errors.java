package com.satan.common.error;

/**
 * @author liuwenyi
 * @date 2020/11/29
 */
public interface Errors {

    /**
     * 获取错误码
     *
     * @return Integer
     */
    Integer getCode();

    /**
     * 获取错误信息
     *
     * @return String
     */
    String getMessage();
}
