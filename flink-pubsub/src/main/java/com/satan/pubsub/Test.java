package com.satan.pubsub;

import java.math.BigDecimal;

/**
 * @author liuwenyi
 * @date 2022/11/18
 **/
public class Test {
    public static void main(String[] args) {
        double aa = 0;
        BigDecimal bigDecimal = BigDecimal.valueOf(aa);
        System.out.println(bigDecimal.equals(BigDecimal.ZERO));
        System.out.println(bigDecimal.compareTo(BigDecimal.ZERO) == 0);
    }
}
