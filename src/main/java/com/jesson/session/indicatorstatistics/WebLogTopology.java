package com.jesson.session.indicatorstatistics;

import com.jesson.session.WebLogConstants;
import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.UUID;

import static com.jesson.session.WebLogConstants.*;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:31
 * @Description:
 */
public class WebLogTopology {
    public static void main(String[] args) {

        WebLogTopology webLogStatictis = new WebLogTopology();
        StormTopology topology = webLogStatictis.buildTopology();

        Config conf = new Config();

        //conf.setNumAckers(4);
        if(args == null || args.length == 0){
            // 本地执行
            conf.setMessageTimeoutSecs(1); // tuple发射超时时间
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("webloganalyse", conf , topology);
        }else{
            // 提交到集群上执行
            conf.setNumWorkers(4); // 指定使用多少个进程来执行该Topology
            try {
                StormSubmitter.submitTopology(args[0],conf, topology);
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 构造一个kafkaspout
     * @return
     */
    private IRichSpout generateSpout(){
        BrokerHosts hosts = new ZkHosts(WebLogConstants.zkConnect);
        String topic = "nginxlog";
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConf = new SpoutConfig(hosts,topic,zkRoot,id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme()); // 按字符串解析
        spoutConf.startOffsetTime = OffsetRequest.LatestTime();;//从头开发消费
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
        return kafkaSpout;
    }




    /**
     *
     * 功能描述: 因为Nginx的日志开发人员做的很不全面，这里无法串联这个日志做深度分析，这里大家在
     * 以后的工作岗位中也要注意职责的对接问题。做PV/UV/Session的时候一定需要关联用户。
     *
     * 这里主要是展示storm读取文件的用法，你去公司会用的着。
     *
     * @param:
     * @return:
     * @auther: jesson
     * @date: 2018/9/4 下午3:47
     */
    private StormTopology buildTopology(){
        // 构造Topology
        TopologyBuilder builder = new TopologyBuilder();
        // 指定Spout
        //builder.setSpout(KAFKA_SPOUT_ID, generateSpout());
        builder.setSpout(KAFKA_SPOUT_ID,new ReadFileSpout(),1);
        builder.setBolt(WEB_LOG_PARSER_BOLT,new LogParserBolt())
                .shuffleGrouping(KAFKA_SPOUT_ID);

        // 将countIPBolt
        builder.setBolt(COUNT_IP_BOLT, new CountKpiBolt(IP_KPI))
                .fieldsGrouping(WEB_LOG_PARSER_BOLT, IP_COUNT_STREAM, new Fields(ClientFromProvince,DAY));

        builder.setBolt(SAVE_BOLT ,new SaveBolt(),3)
                .shuffleGrouping(COUNT_IP_BOLT);
        return builder.createTopology();
    }
}
