package com.jesson.session.behavioraldataanalysis;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Created by Administrator on 4/30.
 */
public class MySplit extends BaseFunction {
    String splitBy = null;
    public MySplit(String splitBy)
    {
        this.splitBy = splitBy;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String orderArr[] = tuple.getString(0).split(splitBy) ;
        String date = orderArr[1].substring(0,10);
        Double amt = Double.parseDouble(orderArr[0]);
        String province_id = orderArr[2];
        System.err.println("****************"+date+"---"+province_id+"---"+amt);
        collector.emit(new Values(date+"_"+province_id,amt));

    }

}