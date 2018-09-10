/*
 * RandomSentenceSpout.java
 * Copyright (C) 2018 white <white@Whites-Mac-Air.local>
 *
 * Distributed under terms of the MIT license.
 */

package com.local.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);
    SpoutOutputCollector _collector; 
    Random _rand;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple(){
        Utils.sleep(100);
        String[] sentences = new String[]{
            sentence("the cow jumped over the moon"), 
            sentence("an apple a day keeps the doctor away"),
            sentence("four score and seven years ago"),
            sentence("snow white and the seven dwarfs"),
            sentence("i am at two with nature")
        };
        final String sentence = sentences[_rand.nextInt(sentences.length)];
        LOG.debug("Emmiting tuple: {}", sentence);
        _collector.emit(new Values(sentence));
    }

    @Override
    public void ack(Object id){

    }
    @Override
    public void fail(Object id){

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word"));
    }

    protected String sentence(String input){
        return input;
    }
}

