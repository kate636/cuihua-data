set mapred.job.name=ods_sc_db_t_shop_inventory_sku_pool;--设置job名
set mapred.job.queue.name=root.etl;--设置跑数队列

-----delete jar hdfs://sfbdp1/tmp/udf/encrypt-1.0.0.jar;
-----add jar hdfs://sfbdp1/tmp/udf/encrypt-1.0.0.jar;
-----这个用于手机号码加密，但是没有做清洗工作，比如空白字符，比如长度小于5的串，比如包含中英文字母的串，都被在加密前【保留】
--create temporary function phone_encrypt as 'com.sf.udf.encrypt.PhoneEncrypt';
-----手机号清洗及加密函数
--create temporary function phone_encrypt_pro as 'com.sf.udf.encrypt.PhoneEncryptPro';

-----这个用于身份证件加密，但是没有做清洗工作。比如空白字符，比如以DE打头的字符串你需要加密，那么可以选择这个UDF
--create temporary function id_encrypt as 'com.sf.udf.encrypt.IdEncrypt';
-----身份证号清洗及加密函数
--create temporary function id_encrypt_pro as 'com.sf.udf.encrypt.IdEncryptPro';

-----这个用于地址加密，做了简单的清洗：剔除地址中间的空白字符
--create temporary function address_encrypt as 'com.sf.udf.encrypt.AddressEncrypt';
-----这个用于地址加密，做了一定程度的清洗：剔除地址中间的特殊字符!@#$%^&* 和空白字符
--create temporary function address_encrypt_pro as 'com.sf.udf.encrypt.AddressEncryptPro';

-----这个UDF绝大多数时候你用不上，这个需要你手动指定加密的方式和数据类型
--create temporary function base_encrypt as 'com.sf.udf.encrypt.EncryptBase';

-----银行卡号加密
--create temporary function credit_encrypt as 'com.sf.udf.encrypt.CreditEncrypt';
-----银行卡号清洗及加密函数
--create temporary function credit_encrypt_pro as 'com.sf.udf.encrypt.CreditEncryptPro';

insert overwrite table `ods_sc_db`.`t_shop_inventory_sku_pool` partition(inc_day='$[time(yyyy-MM-dd,-1d)]')
select `id`,`shop_id`,`inventory_date`,`sku_code`,`sku_name`,`sub_category_id`,`sub_category_name`,`spec`,`sales_unit`,`main_img`,`gift_flag`,`cost_price`,`created_at`,`created_by`,`updated_at`,`updated_by`,`last_updated_at`
from `stg_sc_db`.`t_shop_inventory_sku_pool`
where inc_day = '$[time(yyyy-MM-dd,-1d)]';