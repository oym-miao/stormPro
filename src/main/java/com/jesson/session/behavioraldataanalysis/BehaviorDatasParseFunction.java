package com.jesson.session.behavioraldataanalysis;

import com.ggstar.util.ip.IpHelper;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 *
 * 功能描述: 数据解析、真是公司需要做数据处理、预清洗、等操作
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/15 下午9:11
 */
public class BehaviorDatasParseFunction implements Function {

	private static final long serialVersionUID = -8531306604648164614L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String behaviordatas = tuple.getStringByField("str");

		System.err.println(behaviordatas);
		if(behaviordatas != null && !"".equals(behaviordatas)){
			String[] behaviorDetails = behaviordatas.replace("\"", "").split(" ");
			
			long timestamp = Long.valueOf(behaviorDetails[0]);
			
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
			String user_id = behaviorDetails[1];
			String url = behaviorDetails[2];
			//String ip = behaviorDetails[3];
			String city = behaviorDetails[3];
			Integer http_code = Integer.valueOf(behaviorDetails[4]);
			String refer_url = behaviorDetails[5];
			String client_type = behaviorDetails[6];
			String click_area = behaviorDetails[7];
			Integer total_price = Integer.parseInt(behaviorDetails[8]);
			Integer cheap_price = Integer.parseInt(behaviorDetails[9]);

            /**
             * 解析IP后续扩展数据 需要做数据清洗
			 * "1534603330387" "/IT/139.html" "77.12.128.220" "502" "https://www.sogou.com/web?query=周杰伦" "Iphone6S" "京东文具"
			 */

			//String clientFromCity = IpHelper.findRegionByIp(ip);
			String clientFromCity_yyyyMMddStr = city.trim()+"_"+yyyyMMddStr;

			/**
			 * 对数据进行处理，类似于mysql中的where date>***
			 */


			Values objects = new Values(clientFromCity_yyyyMMddStr, total_price, yyyyMMddStr, yyyyMMddHHStr, yyyyMMddHHmmStr,
					user_id, url, city,http_code, refer_url, client_type, click_area, cheap_price, timestamp);
			System.err.println("==============="+objects.toString());
			collector.emit(objects);

		}

	}

}
