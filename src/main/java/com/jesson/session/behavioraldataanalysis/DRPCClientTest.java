package com.jesson.session.behavioraldataanalysis;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * @Auther: wuyue
 * @Date: 2018/8/22 00:04
 * @Description:
 */
public class DRPCClientTest {
    public static void main(String[] args) {
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("drpc.authorizer.acl.strict",false);
        conf.put("storm.thrift.transport","org.apache.storm.security.auth.SimpleTransportPlugin");
        conf.put("storm.nimbus.retry.times",5);
        conf.put("storm.nimbus.retry.interval.millis",2000);
        conf.put("storm.nimbus.retry.intervalceiling.millis",60000);
        conf.put("drpc.max_buffer_size",1048576);
        try {
            DRPCClient drpc = new DRPCClient(conf,"bigdata03.com", 3772);
            for (int i = 0; i < 60; i++) {
                //"合肥", "重庆", "香港","澳门","兰州","深圳","东莞","浙江", "无锡", "苏州"
                System.err.println("DRPC RESULT: " + drpc.execute("getProvinceSales", "上海_20180823 " +
						"北京_20180823 " +
						"合肥_20180823 " +
						"重庆_20180823 " +
						"香港_20180823 " +
						"澳门_20180823 " +
						"兰州_20180823 " +
						"深圳_20180823 " +
						"东莞_20180823 " +
						"浙江_20180823 " +
                        "无锡_20180823"+
                        "苏州_20180823"));
//                System.err.println("DRPC RESULT: " + drpc.execute("getProvinceSales", "2018-08-22_1 2018-08-22_2 2018-08-22_3 2018-08-22_4 2018-08-22_5 2018-08-22_6 2018-08-22_7 2018-08-22_8"));
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
