package de.hpi.ddm;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class FlinkAssignment {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(4); // how many workers ?
		String textPath = "/home/tom/code/src/github.com/tommartensen/distributed-data-management-vl/src/main/resources/testLog";
		final int delay = 60;					// at most 60 seconds of delay
		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// http://flink.apache.org/docs/latest/apis/streaming/index.html

		SourceFunction<LogEntry> logEntrySource = new LogEntrySource(textPath, delay, servingSpeedFactor);

		DataStream<LogEntry> logEntries = env
				.addSource(logEntrySource);

		logEntries.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
