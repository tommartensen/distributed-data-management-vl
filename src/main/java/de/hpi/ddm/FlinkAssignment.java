package de.hpi.ddm;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;


public class FlinkAssignment {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final int numCores = Integer.parseInt(params.get("cores", "4"));
		final String logPath = params.get("path", "access_log_Aug95");


		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(numCores);

		final int delay = 60;					// at most 60 seconds of delay
		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// http://flink.apache.org/docs/latest/apis/streaming/index.html

		SourceFunction<LogEntry> logEntrySource = new LogEntrySource(logPath, delay, servingSpeedFactor);

		DataStream<LogEntry> logEntries = env.addSource(logEntrySource);

		SingleOutputStreamOperator bytesPerDecade = logEntries
				.filter(entry -> entry.httpMethod.equals("GET") && entry.httpStatus == 200)
				.keyBy("client")
				.timeWindow(Time.seconds(10), Time.seconds(5))
				.sum("bytesTransferred");

		bytesPerDecade.print().setParallelism(1);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
