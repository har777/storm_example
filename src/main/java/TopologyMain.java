import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import bolts.PrinterBolt;
import spouts.LinkDistributor;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("link-distributor",new LinkDistributor());
        builder.setBolt("printer-bolt",new PrinterBolt(),3)
                .shuffleGrouping("link-distributor");
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Twitter-Topology", config , builder.createTopology());

    }
}
