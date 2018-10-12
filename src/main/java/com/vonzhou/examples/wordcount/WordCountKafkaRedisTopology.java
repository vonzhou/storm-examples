/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vonzhou.examples.wordcount;

import com.vonzhou.examples.common.PropertiesUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vonzhou
 */
public class WordCountKafkaRedisTopology {
    public static void main(String[] args) throws Exception {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
                .setHost(PropertiesUtils.getRedisProp("redis.host"))
                .setPort(Integer.parseInt(PropertiesUtils.getRedisProp("redis.port")))
                .setTimeout(Integer.parseInt(PropertiesUtils.getRedisProp("redis.timeout"))).build();

        BrokerHosts zkrHosts = new ZkHosts(PropertiesUtils.getKafkaProp("kafka.zkhosts"));
        final String kafkaTopic = PropertiesUtils.getKafkaProp("kafka.topic");
        final String zkRoot = "/" + kafkaTopic;
        final String spoutId = kafkaTopic;

        SpoutConfig kafkaConfig = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, spoutId);
        // default is byte arr
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 5);
        builder.setBolt("split", new SplitSentenceBolt2(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountRedisBolt(jedisPoolConfig), 12).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            // 设置时间长一点，否则可能看不到运行的输出
            Thread.sleep(20000);
            cluster.shutdown();
        }
    }
}
