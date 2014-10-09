package storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;

import java.util.*;
//作用是twitter 访问2个库一个是TWEETERS_DB   FOLLOWERS_DB
public class TridentReach {
	//用map 模拟的数据库
  public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
    put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
    put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
    put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
  }};

  public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
    put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
    put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
    put("tim", Arrays.asList("alex"));
    put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
    put("adam", Arrays.asList("david", "carissa"));
    put("mike", Arrays.asList("john", "bob"));
    put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
  }};

  public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
    public static class Factory implements StateFactory {
      Map _map;

      public Factory(Map map) {
        _map = map;
      }

      @Override
      public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new StaticSingleKeyMapState(_map);
      }

    }

    Map _map;

    public StaticSingleKeyMapState(Map map) {
      _map = map;
    }


    @Override
    public List<Object> multiGet(List<List<Object>> keys) {
      List<Object> ret = new ArrayList();
      for (List<Object> key : keys) {
        Object singleKey = key.get(0);
        ret.add(_map.get(singleKey));
      }
      return ret;
    }

  }

  public static class One implements CombinerAggregator<Integer> {
    @Override
    public Integer init(TridentTuple tuple) {
      return 1;
    }

    @Override
    public Integer combine(Integer val1, Integer val2) {
      return 1;
    }

    @Override
    public Integer zero() {
      return 1;
    }
  }

  public static class ExpandList extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      List l = (List) tuple.getValue(0);
      if (l != null) {
        for (Object o : l) {
          collector.emit(new Values(o));
        }
      }
    }

  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
	  // 首先定义一个TridentTopology 
    TridentTopology topology = new TridentTopology();
    //在TridentTopology 定义2个TridentState  一个是urlToTweeters  url 和tweeter的关系表 数据源是TWEETERS_DB  一个url 有很多tweeter
    TridentState urlToTweeters = topology.newStaticState(new StaticSingleKeyMapState.Factory(TWEETERS_DB));
    //第二个是tweetersToFollowers 人和关注着
    TridentState tweetersToFollowers = topology.newStaticState(new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));

    //生成drpc的数据流  定义一个函数reach ，进行查询 从 urlToTweeters 进行查询，根据输入的参数  得到tweeters  输入的参数 url 下面的aaa foo.com/blog/1等
    //在每一个tweeters  tweeters 一行是多个的 然后进行ExpandList 打散成一个 生成的字段是tweeter  然后进行shuffle  的作用是进行重新分区
    //然后对每一个tweeter 进行输入 再查询 tweetersToFollowers 里面查  生成字段followers
    //对followers 每一行再打散  成follower  对follower groupby  之后再进行聚合，聚合函数的输入 是follower，输出是个one  new one() 是对每一个follower 返回1 最后sum一下就知道有多少个follower
    //这样就可以传入一个url 就知道有多少个follower
    topology.newDRPCStream("reach", drpc).stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields(
        "tweeters")).each(new Fields("tweeters"), new ExpandList(), new Fields("tweeter")).shuffle().stateQuery(
        tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers")).each(new Fields("followers"),
        new ExpandList(), new Fields("follower")).groupBy(new Fields("follower")).aggregate(new One(), new Fields(
        "one")).aggregate(new Fields("one"), new Sum(), new Fields("reach"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    LocalDRPC drpc = new LocalDRPC();

    Config conf = new Config();
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology("reach", conf, buildTopology(drpc));

    Thread.sleep(2000);
    //trident 很多时候用drpc的方式， 这里是输出客户端drpc的查询
    System.out.println("REACH: " + drpc.execute("reach", "aaa"));
    System.out.println("REACH: " + drpc.execute("reach", "foo.com/blog/1"));
    System.out.println("REACH: " + drpc.execute("reach", "engineering.twitter.com/blog/5"));


    cluster.shutdown();
    drpc.shutdown();
  }
}
