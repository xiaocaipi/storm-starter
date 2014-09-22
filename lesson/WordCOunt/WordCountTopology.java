package WordCOunt;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
//修改的例子会一直运行。
//WordCount 这个bolt开多线程每个线程emit一下   SumBolt 这个bolt只有一个线程就会统计一下，这里算的是单词的pv 和uv ，sumbolt其实就对map 的key和value做循环统计
//因为用的是fieldgroup 所以 就比如单词 a 第一次的时候 WordCount bolt 输出 a 1  sumbolt会进行统计，第二次a来的的时候还是会分配到这个线程上，所以a能加上去
public class WordCountTopology {

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      System.err.println(Thread.currentThread().getName()+"     word="+word+"; count="+count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new MyRandomSentenceSpout(), 1);
    //MySplit 是个工具类的bolt 开多少个线程都无所谓的
    builder.setBolt("split", new MySplit(" "), 8).shuffleGrouping("spout");
    
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
//    builder.setBolt("count", new WordCount(), 12).shuffleGrouping("split");
    //再写一个bolt单线程来对上一个分布式的进行汇总，相当于reduce
    builder.setBolt("sum", new SumBolt() ,1 ).shuffleGrouping("count");
    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

//      Thread.sleep(10000);
//
//      cluster.shutdown();
    }
  }
}
