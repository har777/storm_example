package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class LinkDistributor extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Jedis jedis;
    private boolean completed = false;





    public void close() {

    }


    public void ack(Object msgId) {
        //System.out.println("OK: " + msgId);
        jedis.del(msgId.toString());
        jedis.incr("amazon");
    }

    public void fail(Object msgId) {
        System.out.println("FAIL: " + msgId+"\n\n");
        jedis.lpush("in.amazon",String.valueOf(msgId));
    }

    public void nextTuple(){

        if(completed){
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e){
                System.out.println(e);
            }
            return;
        }

        String key,data;

        key = jedis.rpop("in.amazon");
        if(key != null ) {
            data = jedis.get(key);
            if(data != null){
                this.collector.emit(new Values(data),key);
            }

        }
        else{

           //
        }
    }



    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        jedis = new Jedis("localhost");
        this.collector = collector;
        jedis.set("amazon","0");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("data"));
    }
}
