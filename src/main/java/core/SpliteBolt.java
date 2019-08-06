package core;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpliteBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String record = tuple.getString(1);
        System.out.println("SpliteBolt get record: " + record);
        String[] matcher = record.split("\\s+|-");
        System.out.println("matcher");
        //System.out.println(matcher.length);

        if (matcher.length>=8) {
            System.out.println("SpliteBolt output: &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
            String finish_stamps = matcher[0];
            String time_cost = matcher[1];
            String client_ip = matcher[2];
            String protocol_status = matcher[3];
            String size = matcher[4];
            String request_method = matcher[5];
            String url = matcher[6];
            String trans_hostip = matcher[7];
            String content_type = matcher[8];



//            collector.emit(new Values("finish_stamps", finish_stamps + "##" + remote_addr));
            collector.emit(new Values("time_cost", time_cost));
            collector.emit(new Values("client_ip", client_ip));
            collector.emit(new Values("protocol_status", protocol_status));
            collector.emit(new Values("size", size));
            collector.emit(new Values("request_method", request_method));
            collector.emit(new Values("url", url));
            collector.emit(new Values("trans_hostip", trans_hostip));
            collector.emit(new Values("content_type", content_type));
        }
        else {
            System.out.println("NO MATCH");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("item", "value"));
    }
}
