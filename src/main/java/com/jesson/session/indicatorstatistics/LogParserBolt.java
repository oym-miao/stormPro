package com.jesson.session.indicatorstatistics;

import com.ggstar.util.ip.IpHelper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.jesson.session.WebLogConstants.*;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:35
 * @Description:
 */
public class LogParserBolt implements IRichBolt {
    private Pattern pattern;
    private OutputCollector  collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //nginx日志文件的解析规则
        pattern = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) (\\[.*\\]) (\\\".*?\\\")" +
                " (-|[0-9]*) (-|[0-9]*) (\\\".*?\\\") (\\\".*?\\\")");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String webLog = input.getStringByField("log");
        // 解析日志数据
        try {
            if (webLog != null && !"".equals(webLog)) {
                Matcher matcher = pattern.matcher(webLog);
                if (matcher.find()) {
                    //matcher.group(0); 注意这里的异常
                    String ip = matcher.group(1);
                    String serverTimeStr = matcher.group(4);

                    // 处理时间
                    if(!"".equals(serverTimeStr)) {
                        //获取原始数据
                        serverTimeStr = serverTimeStr.substring(1, serverTimeStr.length() - 1);
                        SimpleDateFormat sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);
                        //数据清洗 时间细粒度处理
                        Date time = sourceFormat.parse(serverTimeStr);

                        SimpleDateFormat transferFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String dateStr = transferFormat.format(time);

                        DateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
                        String yyyyMMddStr = yyyyMMdd.format(time);

                        DateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
                        String yyyyMMddHHStr = yyyyMMddHH.format(time);

                        DateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
                        String yyyyMMddHHmmStr = yyyyMMddHHmm.format(time);

                        //解析IP为省份名
                        String clientfromprovince = IpHelper.findRegionByIp(ip).trim(); //注意这里的坑

                        System.err.println("=======解析出来的时间为======="+yyyyMMddStr+"===>"+yyyyMMddHHStr+"=====>"+yyyyMMddHHmmStr+"===>"+clientfromprovince+"===>"+ip);

                        String requestUrl = matcher.group(5);
                        String httpRefer = matcher.group(6);
                        String userAgent = matcher.group(9);


                        // 分流 streamId 如果这里使用了streamid,调用的时候要注意
                        //我这里做一个Ip统计的
                        this.collector.emit(IP_COUNT_STREAM, input, new Values(yyyyMMddStr, yyyyMMddHHStr, yyyyMMddHHmmStr, clientfromprovince));
                        this.collector.emit(URL_PARSER_STREAM, input, new Values(yyyyMMddStr, yyyyMMddHHStr, yyyyMMddHHmmStr, requestUrl));
                        this.collector.emit(HTTPREFER_PARSER_STREAM, input, new Values(yyyyMMddStr, yyyyMMddHHStr, yyyyMMddHHmmStr, httpRefer));
                        this.collector.emit(USERAGENT_PARSER_STREAM, input, new Values(yyyyMMddStr, yyyyMMddHHStr, yyyyMMddHHmmStr, userAgent));
                    }
                }
            }
            this.collector.ack(input);
        }catch (Exception e){
            this.collector.fail(input);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(IP_COUNT_STREAM,new Fields(DAY, HOUR, MINUTE, ClientFromProvince));
        declarer.declareStream(URL_PARSER_STREAM,new Fields(DAY, HOUR, MINUTE, REQUEST_URL));
        declarer.declareStream(HTTPREFER_PARSER_STREAM,new Fields(DAY, HOUR, MINUTE, HTTP_REFER));
        declarer.declareStream(USERAGENT_PARSER_STREAM,new Fields(DAY, HOUR, MINUTE, USERAGENT));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
