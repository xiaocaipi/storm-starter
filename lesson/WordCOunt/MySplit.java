package WordCOunt;


import java.util.Map;

import com.sun.java_cup.internal.runtime.virtual_parse_stack;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MySplit implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String patton ;
	public MySplit(String patton)
	{
		this.patton = patton;
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			String sen = input.getString(0);
			if(sen != null)
			{
				for(String word : sen.split(patton))
				{
					collector.emit(new Values(word));
				}
			}
			//这里集成的是IBasicBolt 没有ack 和fail 自动是ack 如果要异常就捕获FailedException 就可以
		} catch (FailedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		// TODO Auto-generated method stub
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
