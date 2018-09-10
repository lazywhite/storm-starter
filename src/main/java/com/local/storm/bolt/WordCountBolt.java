/*
 * PrintBolt.java
 * Copyright (C) 2018 white <white@Whites-Mac-Air.local>
 *
 * Distributed under terms of the MIT license.
 */
package com.local.storm.bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.HashMap;


@SuppressWarnings("serial")
public class WordCountBolt implements IRichBolt{
    Map<String, Integer> counters;
    private OutputCollector collector;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector){
        collector = outputCollector;
        counters = new HashMap<String, Integer>();
    }
    public void execute(Tuple input){
        String word = input.getString(0);
        if(!counters.containsKey(word)){
            counters.put(word, 1);
        }else{
            Integer c = counters.get(word) + 1;
            counters.put(word, c);
        }

        collector.emit(new Values(word, counters.get(word)));
    }

    public void cleanup(){}
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word", "count"));
    }
    public Map<String, Object> getComponentConfiguration(){
        return null;
    }
     
}

