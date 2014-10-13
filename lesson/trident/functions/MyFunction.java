package trident.functions;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import tools.DateFmt;
import backtype.storm.tuple.Values;

public class MyFunction extends BaseFunction {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		for(int i=0; i < tuple.getInteger(0); i++) {
		 collector.emit(new Values(i));
	  }
	 }
  }
