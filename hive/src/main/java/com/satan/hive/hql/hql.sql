/**
 * @author liuwenyi
 * @date 2020/11/29
 */
 -- 创建数据库

CREATE DATABASE IF NOT EXISTS hive_test;

-- 使用数据库
USE hive_test;

-- 创建 订单表，指定分隔符
CREATE TABLE IF NOT EXISTS `order`(
    order_id string,
    user_id string,
    eval_set string,
    order_number string,
    order_dow string,
    order_hour_of_day string,
    days_since_prior_order string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- 从 HDFS 上加载数据
load data inpath '/user/root/hive_data/order.txt' into table hive_test.`order`;

-- 创建 产品表
CREATE TABLE IF NOT EXISTS `product`(
    order_id string,
    product_id string,
    add_to_cart_order string,
    reordered string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- 从本地加载数据
load data LOCAL inpath '/root/data/product.txt'  into table hive_test.`product`;

/**
   统计每个用户的订单数
 */
SELECT a.user_id,COUNT(a.order_id) AS orderCount
FROM hive_test.`order` AS a
GROUP BY a.user_id;

/**
  统计每个用户平均每个订单多少商品
 */
SELECT a.user_id,count(b.product_id)/count(a.order_id)
FROM hive_test.`order` AS a
         LEFT JOIN hive_test.`product` AS b ON a.order_id = b.order_id
GROUP BY a.user_id



