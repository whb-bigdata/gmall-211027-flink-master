package com.atguigu.bean;

import lombok.Data;

@Data
public class OrderInfoRefundBean {
    // 订单 id
    String id;
    // 省份 id
    String province_id;
    // 历史数据
    String old;
}
