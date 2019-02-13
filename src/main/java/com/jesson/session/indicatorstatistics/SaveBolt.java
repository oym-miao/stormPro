package com.jesson.session.indicatorstatistics;

import com.jesson.session.dao.HBaseDAO;
import com.jesson.session.dao.imp.HBaseDAOImp;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static com.jesson.session.WebLogConstants.*;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:43
 * @Description:
 */
public class SaveBolt implements IRichBolt {
    private HBaseDAO hBaseDAO;
    private long starttime;
    private long endtime;
    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        starttime = System.currentTimeMillis();
        hBaseDAO = new HBaseDAOImp();
        this._collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String serverTimeAndKpi = input.getStringByField(SERVERTIME_KPI);
            Integer kpiCounts = input.getIntegerByField(KPI_COUNTS);
            String columnQuelifier = serverTimeAndKpi.split("_")[0];
            System.err.println("serverTimeAndKpi=" + serverTimeAndKpi + ", kpiCounts=" + kpiCounts);
            endtime = System.currentTimeMillis();
                hBaseDAO.insert("test2", serverTimeAndKpi, COLUMN_FAMILY, columnQuelifier, "" + kpiCounts);
                starttime = System.currentTimeMillis();
            this._collector.ack(input);
        }catch (Exception e){
            this._collector.fail(input);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
