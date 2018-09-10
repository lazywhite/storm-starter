/*
 * PrintBolt.java
 * Copyright (C) 2018 white <white@Whites-Mac-Air.local>
 *
 * Distributed under terms of the MIT license.
 */
package com.local.storm.bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import java.util.Map;


@SuppressWarnings("serial")
public class WordNormalizerBolt implements IRichBolt{
    private OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector){
        collector = outputCollector;
    }
    public void execute(Tuple input){
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word: words){
            collector.emit(new Values(word));
        }
    }

    public void cleanup(){}
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word"));
    }
    public Map<String, Object> getComponentConfiguration(){
        return null;
    }
     
}

