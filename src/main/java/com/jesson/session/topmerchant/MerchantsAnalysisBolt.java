package com.jesson.session.topmerchant;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * @Auther: jesson
 * @Date: 2018/9/6 14:44
 * @Description:
 */
public class MerchantsAnalysisBolt extends BaseRichBolt {


    private OutputCollector _collector;
    MerchantMessageHandler loginfohandler;

    public void execute(Tuple tuple) {
        Jedis conn = JedisUtil.getConn();
        String loginfo = tuple.getStringByField("log");
        OrdersBean order = loginfohandler.getOrdersBean(loginfo);

        //多维度统计
        // 比如电商会有多个店家 那么根据店家维度进行统计
        //每个店铺的总销售额
        //Redis的rowKey设计 orderservice:merchant:price:date
        //每个店铺的购买人数
        //Redis的rowKey设计 orderservice:merchant:user:date
        //每个店铺的销售数量
        //Redis的rowKey设计 orderservice:merchant:num:date

        conn.incrByFloat("orderservice:"+order.getMerchantName()+":price:"+order.getCreateTime(), order.getTotalPrice());
        conn.incrBy("orderservice:"+order.getMerchantName()+":user:"+order.getCreateTime(),1);
        conn.incrBy("orderservice:"+order.getMerchantName()+":num:"+order.getCreateTime(),1);

        //思考平台维度 总销售额 多少人下单 买了多少件商品

        //商品的维度 苹果电脑火爆吗？今天卖了多少 多少人买 卖出多少金额

        conn.close();

    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this._collector = collector;
        this.loginfohandler = new MerchantMessageHandler();
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {}

}
