package com.orange.marwan.VoiceEventConsumer;

import com.orange.marwan.VoiceEventConsumer.objects.OEGEvent;
import org.apache.flink.api.common.functions.ReduceFunction;

public class DataAggregation implements ReduceFunction<OEGEvent> {

	@Override
	public OEGEvent reduce(OEGEvent value1, OEGEvent value2) throws Exception {
		System.out.println("this is Test");
		return value2;
	}

}
