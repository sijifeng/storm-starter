package com.storm.sijifeng.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by yingchun on 2018/6/1.
 */
public class KafkaBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare (Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    @Override
    public void execute (Tuple input) {
        System.out.println("input = [" + input + "]");
        //collector.ack(input);
    }
}