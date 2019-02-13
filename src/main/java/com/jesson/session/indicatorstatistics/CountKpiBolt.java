package com.jesson.session.indicatorstatistics;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.jesson.session.WebLogConstants.KPI_COUNTS;
import static com.jesson.session.WebLogConstants.SERVERTIME_KPI;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:38
 * @Description:
 */
public class CountKpiBolt implements IRichBolt{
    private String kpiType;
    //第一种实现方式 内存保存
    private ConcurrentMap<String,Integer> kpiCounts;

    private String currentDay = "";

    private OutputCollector _collector;

    public CountKpiBolt(String kpiType){
        this.kpiType = kpiType;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.kpiCounts = new ConcurrentHashMap<>();
        this._collector = collector;
    }



    @Override
    public void execute(Tuple input) {
        String day = input.getStringByField("day");
        String hour = input.getStringByField("hour");
        String minute = input.getStringByField("minute");
        String kpi = input.getString(3);

        String kpiByDay = day + "_" + kpi;
        String kpiByHour = hour +"_" + kpi;
        String kpiByMinute = minute + "_" + kpi;



        // 隔天清理内存，防止内存爆了
        if(!currentDay.equals(day)){
            Iterator<Map.Entry<String,Integer>> iter = kpiCounts.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry<String,Integer> entry = iter.next();
                if(entry.getKey().startsWith(currentDay+"_")){
                    iter.remove();
                }
            }
        }

        currentDay = day;

        int kpiCountByDay = 0;
        int kpiCountByHour = 0;
        int kpiCountByMinute = 0;

        if(kpiCounts.containsKey(kpiByDay)){
            kpiCountByDay = kpiCounts.get(kpiByDay);
        }
        if(kpiCounts.containsKey(kpiByHour)){
            kpiCountByHour = kpiCounts.get(kpiByHour);
        }

        if(kpiCounts.containsKey(kpiByMinute)){
            kpiCountByMinute = kpiCounts.get(kpiByMinute);
        }

        kpiCountByDay ++;
        kpiCountByHour ++;
        kpiCountByMinute ++;

        kpiCounts.put(kpiByDay, kpiCountByDay);
        kpiCounts.put(kpiByHour, kpiCountByHour);
        kpiCounts.put(kpiByMinute,kpiCountByMinute);

        this._collector.emit(input, new Values(kpiType+"_" + kpiByDay, kpiCountByDay));
        this._collector.emit(input, new Values(kpiType+"_" + kpiByHour, kpiCountByHour));
        this._collector.emit(input, new Values(kpiType+"_" + kpiByMinute, kpiCountByMinute));
        this._collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SERVERTIME_KPI, KPI_COUNTS));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
