package com.storm.sijifeng.kafka;

import com.storm.sijifeng.kafka.bolt.KafkaProducerTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * Created by yingchun on 2018/6/1.
 */
public class StormKafka {
    public static void main (String[] args) throws Exception {
        new StormKafka().runMain(args);
    }


    protected void runMain (String[] args) throws Exception {
        String bootstrapServers = "192.168.0.22:29092,192.168.0.22:39092,192.168.0.22:49092";
        String topic = "prism-test";
        Config tpConf = getConfig();

        if (args.length > 0) {
            // Producers. This is just to get some data in Kafka, normally you would be getting this data from elsewhere
            //StormSubmitter.submitTopology(topic + "-producer", tpConf, KafkaProducerTopology.newTopology(bootstrapServers, topic));
            //Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
            StormSubmitter.submitTopology("storm-kafka-client-spout-test", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(bootstrapServers)));
        } else {
            LocalCluster cluster = new LocalCluster();
            // Producers. This is just to get some data in Kafka, normally you would be getting this data from elsewhere
            //cluster.submitTopology(topic + "-producer", tpConf, KafkaProducerTopology.newTopology(bootstrapServers, topic));
            //Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
            cluster.submitTopology("storm-kafka-client-spout-test", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(bootstrapServers)));
        }

    }

    protected StormTopology getTopologyKafkaSpout (KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("kafka_bolt", new KafkaBolt())
                .shuffleGrouping("kafka_spout", "aa");
        //tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
        return tp.createTopology();
    }


    protected Config getConfig () {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig (String bootstrapServers) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"), "aa");

        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{"prism-kafka-test"})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }


    protected KafkaSpoutRetryService getRetryService () {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
