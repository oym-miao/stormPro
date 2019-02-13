package com.jesson.session.topmerchant;

import java.io.Serializable;

/**
 * @Auther: jesson
 * @Date: 2018/9/6 14:49
 * @Description:
 */
public class OrdersBean implements Serializable {
    private static final long serialVersionUID = -4887995756541608543L;
    private String createTime;
    private String merchantName;
    private double totalPrice;
    private double discountPrice;

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getMerchantName() {
        return merchantName;
    }

    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public double getDiscountPrice() {
        return discountPrice;
    }

    public void setDiscountPrice(double discountPrice) {
        this.discountPrice = discountPrice;
    }
}
