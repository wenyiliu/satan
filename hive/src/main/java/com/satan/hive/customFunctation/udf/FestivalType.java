package com.satan.hive.customFunctation.udf;

import com.satan.common.error.SatanError;
import com.satan.common.exception.BizException;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author liuwenyi
 * @date 2020/11/24
 */
public final class FestivalType extends UDF {

    public static void main(String[] args) {
        throw new BizException(SatanError.TABLE_IS_NOT_DROP);
    }

}
