/*
 * WordCountTopology.java
 * Copyright (C) 2018 white <white@Whites-Mac-Air.local>
 *
 * Distributed under terms of the MIT license.
 */

package com.local.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import com.local.storm.spout.RandomSentenceSpout;
import com.local.storm.bolt.WordNormalizerBolt;
import com.local.storm.bolt.WordCountBolt;
import com.local.storm.bolt.PrintBolt;


public class WordCountTopology {
    private static TopologyBuilder builder = new TopologyBuilder();
     
    public static void main(String[] args){
        Config config = new Config();
        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("WordNormalizer", new WordNormalizerBolt(), 2).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(), 2).fieldsGrouping("WordNormalizer", new Fields("word"));
        builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping("WordCount");
        config.setDebug(true);

        if(args != null && args.length > 0){
            try{
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            }catch(Exception e){
                e.printStackTrace();
            }
        }else{
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", config, builder.createTopology());
        }
    }
}

