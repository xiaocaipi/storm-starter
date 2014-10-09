package storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;


public class TridentWordCount {
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
	  //FixedBatchSpout implements 了 ibatchspout 这个是trident 下的 基础的spout  事务型的叫itridentspout
	  //把其组成batch 发射出去
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
        new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
        new Values("how many apples can you eat"), new Values("to be or not to be the person"));
    //setCycle 是true的话 会不断的循环
    spout.setCycle(true);
    //TridentTopology 是trident 的topo
    TridentTopology topology = new TridentTopology();
    //创建一个流，把spout作为数据源，parallelismHint 是并发度，
    //each一下是对每一行 sentence 的每一行就是上面发射的 然后每一行进行split一下，输出一个word ，
    //groupby了之后  进行存储一下 这里的存储是个map里面 存储是按照Count 计数，字段是count，持久persistentAggregate 返回是个TridentState 
    TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
        new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
        new Count(), new Fields("count")).parallelismHint(16);
    //topology 生成一个drpc的流  drpc必须要告诉一个函数的名称，然后接受客户端的参数，客户端的参数有多个这个切分一下，输出word 字段，
    //之后堆word进行groupby，groupby之后没有聚集函数的话作用就是去重
    //之后进行stateQuery 进行查询，查询的时候传入一个TridentState TridentState 是一种中间存储，中间存储可以用内存，关系型数据库都可以   就是在TridentState 进行查询
    //TridentState 里面随着流的不断过来数据是在不断的更新的，与此同时这里进行查询 ，查询传进来分隔的参数 MapGet 就是把查询的结果进行一个输出，输出的字段是count
    //查出来之后 每行进行 对输入的count  进行FilterNull 过滤 把count  是空的过滤，最后做一个aggregate 集合，
    //聚合的输入是过滤好的count，操作是进行sum 输出的sum
    topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields(
        "word")).stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")).each(new Fields("count"),
        new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      //提交TridentWordCount
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
        Thread.sleep(1000);
      }
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
