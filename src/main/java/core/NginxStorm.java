package core;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class NginxStorm {
    public static void main(String[] argv) throws InterruptedException {

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("LogSpout", new LogSpout(), 1);
        builder.setBolt("SpliteBolt", new SpliteBolt(), 1).shuffleGrouping("LogSpout");
        builder.setBolt("CounterBolt", new CounterBolt(), 1)
                .fieldsGrouping("SpliteBolt", new Fields("item"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("NginxLog", config, builder.createTopology());
        Thread.sleep(10000);

        cluster.killTopology("NginxLog");
        cluster.shutdown();
    }
}