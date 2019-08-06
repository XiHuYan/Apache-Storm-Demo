package core;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LogSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private static String srcFile = "/home/jinzuoyou/Desktop/access.log";
    private static long index = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.context = topologyContext;
    }

    public void nextTuple() {
        long lines = 0;
        String record = null;

        try {
            Stream<String> fileStream = Files.lines(Paths.get(srcFile));
            lines = fileStream.count();
            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (lines != index) {
            if (lines < index)
                index = 0;
            System.out.println("LogSpout output: &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
            System.out.println(lines);
            for (long i = index; i < lines; i++) {
                try {
                    List<String> fileStream = Files.readAllLines(Paths.get(srcFile));      //wtf? trash
                    record = fileStream.get((int)i);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                this.collector.emit(new Values(i, record));     //line-id, line-text of log
            }
            index = lines;
        }
        else {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("index", "record"));
    }
}
