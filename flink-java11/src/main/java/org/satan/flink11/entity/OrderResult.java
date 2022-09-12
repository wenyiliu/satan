package org.satan.flink11.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author liuwenyi
 * @date 2022/09/11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResult {

    private String name;

    private Integer type;

    private BigDecimal amount;

    private Integer num;
}
