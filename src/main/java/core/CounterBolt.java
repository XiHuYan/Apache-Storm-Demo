package core;

import regx.UserAgent;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;


public class CounterBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        System.out.println("CounterBolt get: " + tuple);
        String item = tuple.getString(0);
        String value = tuple.getString(1);

        switch (item) {
            // http 状态码统计
            case "client_ip" :
                System.out.println("sent from: " + value);
                break;

            // 客户端信息
            case "url" :
                System.out.println("request url: " + value);
                break;

            // all pass
            default:
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("item"));
    }
}
