package com.satan.kafka.entity;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author liuwenyi
 * @date 2022/09/11
 */
@Data
@Builder
public class Order {

    private Integer id;

    private BigDecimal amount;

    private Long timestamp;

    private Integer type;

    private String name;
}
