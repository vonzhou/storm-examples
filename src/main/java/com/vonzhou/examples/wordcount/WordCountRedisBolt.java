package com.vonzhou.examples.wordcount;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

public class WordCountRedisBolt extends AbstractRedisBolt {

    private static final String COUNT_KEY = "cnt.%s";

    public WordCountRedisBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    protected void process(Tuple tuple) {
        String w = tuple.getStringByField("word");

        Jedis jedis = (Jedis) getInstance();
        jedis.incr(String.format(COUNT_KEY, w));

        returnInstance(jedis);
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
