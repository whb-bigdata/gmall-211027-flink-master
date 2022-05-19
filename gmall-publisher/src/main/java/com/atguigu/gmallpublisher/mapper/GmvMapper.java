package com.atguigu.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface GmvMapper {

    //查询ClickHouse,获取GMV总数
    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    Double selectGmv(int date);

}
