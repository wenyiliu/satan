package com.satan.common.exception;

import com.satan.common.error.Errors;

/**
 * @author liuwenyi
 * @date 2020/11/29
 */
public class BizException extends RuntimeException {

    private final Integer code;

    public BizException(Errors errors) {
        super(errors.getMessage());
        this.code = errors.getCode();
    }

    public BizException(Integer code, String message) {
        super(message);
        this.code = code;
    }

    public Integer getCode() {
        return this.code;
    }
}
