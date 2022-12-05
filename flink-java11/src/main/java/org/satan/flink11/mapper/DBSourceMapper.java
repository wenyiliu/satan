package org.satan.flink11.mapper;


import org.apache.ibatis.annotations.Select;
import org.satan.flink11.entity.DBSource;

/**
 * @Classname DBSourceMapper
 * @Date 2020/11/24 下午3:11
 * @Created by Zalex
 */
public interface DBSourceMapper {

    @Select("SELECT * FROM flink_db_source WHERE `key` = #{key}")
    DBSource selectByKey(String key);

}

