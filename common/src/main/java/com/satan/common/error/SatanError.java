package com.satan.common.error;

/**
 * @author liuwenyi
 * @date 2020/11/29
 */
public enum SatanError implements Errors {

    /**
     * hive 错误码
     */

    TABLE_IS_NOT_DROP(1000, "表不能被删除"),
    ;

    private Integer code;


    private String message;

    SatanError(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
