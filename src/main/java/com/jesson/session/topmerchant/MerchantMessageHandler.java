package com.jesson.session.topmerchant;

import org.apache.storm.shade.org.apache.http.util.TextUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Auther: jesson
 * @Date: 2018/9/6 14:46
 * @Description:
 */
public class MerchantMessageHandler {

    /**
     *
     * 功能描述: 从日志流中在线解析商家信息和订单信息
     *
     * @param:
     * @return:
     * @auther: jesson
     * @date: 2018/9/6 下午3:03
     */
    public OrdersBean getOrdersBean(String loginfo) {

        OrdersBean order = new OrdersBean();
        //从日志信息中过滤出订单信息
        if(!TextUtils.isEmpty(loginfo)) {

            String[] orderDetails = loginfo.replace("\"", "").split(" ");
            long timestamp = Long.valueOf(orderDetails[0]);
            Date date = new Date(timestamp);
            /**
             * 为了业务更加精准，我们需要对时间进行年月日时分秒的处理，方便后续stream的处理
             */

            DateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
            String yyyyMMddStr = yyyyMMdd.format(date);

            DateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
            String yyyyMMddHHStr = yyyyMMddHH.format(date);


            DateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
            String yyyyMMddHHmmStr = yyyyMMddHHmm.format(date);

            String consumer = orderDetails[1];
            String access_url = orderDetails[2];
            String province = orderDetails[3];
            String client_type = orderDetails[6];

            String merchantName = orderDetails[10];

            double totalprice = Double.valueOf(orderDetails[8]);
            double discountprice = Double.valueOf(orderDetails[9]);


            //"1536215867209" "0003" "/IT/139.html" "14.196.255.220" "500" "https://www.so.com/s?q=周杰伦" "MacPro2017" "京东文具" "86" "28" "BMW"

            //获取创建时间
            order.setCreateTime(yyyyMMddStr);
            //获取商家名称
            order.setMerchantName(merchantName);
            //获取订单总额
            order.setTotalPrice(totalprice);
            //获取订单折扣金额
            order.setDiscountPrice(discountprice);

            return order;
        }

        return order;
    }
}
