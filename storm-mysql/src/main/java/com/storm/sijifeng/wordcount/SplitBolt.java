package com.storm.sijifeng.wordcount;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by yingchun on 2018/5/23.
 */
public class SplitBolt extends BaseBasicBolt {
    @Override
    public void execute (Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String string = tuple.getStringByField("demo-storm");
        String[] split = string.split(" ");
        for (String s : split) {
            basicOutputCollector.emit(new Values(s, 1));
        }
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "num"));
    }
}
