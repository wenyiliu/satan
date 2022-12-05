package org.satan.flink11.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author liuwenyi
 * @date 2022/10/15
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HBaseRowData {

    private String namespace;

    private String tableName;

    private String rowkey;

    private Map<String, Map<String, Object>> rowMap;

    private long bufferFlushMaxSizeInBytes = 10000L;

    private long bufferFlushIntervalMillis = 1L;

    private long bufferFlushMaxMutations = 10L;

}
