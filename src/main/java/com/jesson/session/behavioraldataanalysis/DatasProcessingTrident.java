package com.jesson.session.behavioraldataanalysis;

import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;



/**
 *
 * 功能描述: topology
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/11 上午12:31
 */
public class DatasProcessingTrident {

	private String zkUrl;
	private String brokerUrl;
	private String topic;

	public DatasProcessingTrident(String zkUrl, String brokerUrl, String topic) {
		this.zkUrl = zkUrl;
		this.brokerUrl = brokerUrl;
		this.topic = topic;
	}

	/**
	 *
	 * 功能描述: 构造一个kafka的trident 1
	 *
	 * @param:
	 * @return:
	 * @auther: jesson
	 * @date: 2018/9/12 下午4:57
	 */
	private TransactionalTridentKafkaSpout createKafkaSpout() {
		ZkHosts hosts = new ZkHosts(zkUrl);
		TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic);
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.ignoreZkOffsets = true;
		//config.forceFromStart = *false*; 0.*版本
		config.startOffsetTime = OffsetRequest.LatestTime();//从最新的开始消费
		return new TransactionalTridentKafkaSpout(config);
	}
	/**
	 *
	 * 功能描述: 构建一个Storm Topology 2
	 *
	 * @param:
	 * @return:
	 * @auther: jesson
	 * @date: 2018/9/12 下午5:01
	 */
	public StormTopology buildConsumerTopology(LocalDRPC drpc) {
		TridentTopology tridentTopology = new TridentTopology();
		addDRPCStream(tridentTopology, addTridentState(tridentTopology), drpc);
		return tridentTopology.build();
	}
	/**
	 *
	 * 功能描述: 构建TridentState 3
	 *
	 * @param:
	 * @return:
	 * @auther: jesson
	 * @date: 2018/9/12 下午5:05
	 */
	private TridentState addTridentState(TridentTopology tridentTopology) {
		TridentState tridentState = tridentTopology.newStream("spout1", createKafkaSpout()).parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY), new BehaviorDatasParseFunction(),
						new Fields("clientFromCity_yyyyMMddStr", "total_price"))
				.each(new Fields("clientFromCity_yyyyMMddStr","total_price"),new PrintTestFilter())
				//指标合并 把日期和城市名字合并
				.groupBy(new Fields("clientFromCity_yyyyMMddStr"))
				//这里用MemoryMapState 不放在Hbase
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("total_price"), new Sum(), new Fields("result"));


		return tridentState;
	}
	/**
	 *
	 * 功能描述: 构造一个DRPC Stream 4
	 *
	 * @param:
	 * @return:
	 * @auther: jesson
	 * @date: 2018/9/12 下午5:02
	 */
	private Stream addDRPCStream(TridentTopology tridentTopology, TridentState state, LocalDRPC drpc) {
		return tridentTopology.newDRPCStream("getProvinceSales", drpc)
				.each(new Fields("args"), new Split(), new Fields("clientFromCity_yyyyMMddStr"))
				.groupBy(new Fields("clientFromCity_yyyyMMddStr"))
				.stateQuery(state, new Fields("clientFromCity_yyyyMMddStr"), new MapGet(), new Fields("result"))
				.each(new Fields("result"), new FilterNull())
				.project(new Fields("clientFromCity_yyyyMMddStr", "result"))
				.each(new Fields("clientFromCity_yyyyMMddStr", "result"),new PrintTestFilter())
				.applyAssembly(new FirstN(5,"result",true));
	}

	public static void main(String[] args) throws Exception {

		String zkUrl = "bigdata01.com:2181,bigdata02.com:2181,bigdata03.com:2181";
		String brokerUrl = "bigdata02.com:2181";
		String topic = "order_top6";

		DatasProcessingTrident wordCount = new DatasProcessingTrident(zkUrl, brokerUrl,topic);
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxSpoutPending(20);
		if (args.length > 0)  {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0] , conf, wordCount.buildConsumerTopology(null));
		} else {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("dataprocess", conf, wordCount.buildConsumerTopology(drpc));
            for (int i = 0; i < 60; i++) {
				System.err.println("drpc_result: " + drpc.execute("getProvinceSales", "北京_20180926 上海_20180926 合肥_20180926 重庆_20180926 香港_20180926 澳门_20180926 兰州_20180926 深圳_20180926"));
				Thread.sleep(3000);
            }
		}
	}
}
