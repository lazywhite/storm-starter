/*
 * PrintBolt.java
 * Copyright (C) 2018 white <white@Whites-Mac-Air.local>
 *
 * Distributed under terms of the MIT license.
 */
package com.local.storm.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt{
    public void execute(Tuple input, BasicOutputCollector collector){
        /*
        try{
            string msg = input.getString(0);
            if(msg != null){
                System.out.println(msg);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        */
        System.out.println(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
    }
     
}

