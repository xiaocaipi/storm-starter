package lesson;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
//bolt 这里统计一个行数，对spout 传过来的数据
public class MyBolt implements IRichBolt {

	OutputCollector collector = null;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	int num = 0;
	String valueString = null;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
//		input.getValueByField("log");
//		input.getValue(0);
		try {
			valueString = input.getStringByField("log") ;
			
			if(valueString != null)
			{
				num ++ ;
				//加线程号来区分分组策略
				System.err.println(Thread.currentThread().getName()+"	lines  :"+num +"   session_id:"+valueString.split("\t")[1]);
			}
			collector.ack(input);
//			Thread.sleep(2000);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector ;
	}

	@Override
	//定义输出的字段，如果没有下一级的bolt 这里可以不用写
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("")) ;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
