package com.jesson.session.topmerchant;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * @Auther: jesson
 * @Date: 2018/9/6 14:39
 * @Description:
 */
public class OrderAnalysisTopology {
    private static String topicName = "ordersInfo";
    private static String zkRoot = "/stormKafka/"+topicName;

    public static void main(String[] args) {

        BrokerHosts hosts = new ZkHosts("bigdata01.com:2181,bigdata02.com:2181,bigdata03.com:2181");


//        SpoutConfig spoutConfig = new SpoutConfig(hosts,topicName,zkRoot, UUID.randomUUID().toString());
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("fileSpout",new ReadFileSpout());
        builder.setBolt("merchantsSalesBolt", new MerchantsAnalysisBolt(), 2).shuffleGrouping("fileSpout");

        Config conf = new Config();
        conf.setDebug(true);

        if(args != null && args.length > 0) {
            conf.setNumWorkers(1);
            try {
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }

        } else {
            conf.setMaxSpoutPending(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ordersAnalysis", conf, builder.createTopology());
        }

    }
}
