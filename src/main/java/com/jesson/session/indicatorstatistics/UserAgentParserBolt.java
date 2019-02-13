package com.jesson.session.indicatorstatistics;

import com.jesson.session.utils.UserAgentUtil;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static com.jesson.session.WebLogConstants.*;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:41
 * @Description:
 */
public class UserAgentParserBolt implements IRichBolt {
    private OutputCollector _collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String day = input.getStringByField(DAY);
        String hour = input.getStringByField(HOUR);
        String minute = input.getStringByField(MINUTE);
        String userAgent = input.getStringByField(USERAGENT);

        // 解析userAgent
        if(userAgent != null && !"".equals(userAgent)){
            UserAgentInfo userAgentInfo =
                    UserAgentUtil.analyticUserAgent(userAgent);

            if(userAgentInfo!= null){

                String browserName = userAgentInfo.getUaFamily();
                String browserVersion = userAgentInfo.getBrowserVersionInfo();

                if(browserName!=null && !"".equals(browserName)){
                    // 只考虑浏览器的类型
                    this._collector.emit(BROWSER_COUNT_STREAM,input,new Values(day,hour,minute,browserName));

                    if(browserVersion!=null && !"".equals(browserVersion)){
                        this._collector.emit(BROWSER_COUNT_STREAM,input,new Values(day,hour,minute,
                                browserName+"_"+browserVersion));
                    }
                }

                String osName = userAgentInfo.getOsName();
                String osVersion = userAgentInfo.getOsFamily();

                if(osName!= null && !"".equals(osName)){
                    this._collector.emit(OS_COUNT_STREAM,input,new Values(day,hour,minute, osName));

                    if(osVersion != null && !"".equals(osVersion)){
                        this._collector.emit(OS_COUNT_STREAM,input,new Values(day,hour,minute,osName+"_"+osVersion));
                    }
                }
            }
        }

        this._collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(BROWSER_COUNT_STREAM,new Fields(DAY,HOUR,MINUTE,BROWSER));
        declarer.declareStream(OS_COUNT_STREAM,new Fields(DAY,HOUR,MINUTE,OS));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
