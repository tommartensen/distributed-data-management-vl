package de.hpi.ddm;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.function.LongBinaryOperator;


public class FlinkAssignment {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final int numCores = Integer.parseInt(params.get("cores", "4"));
		final String logPath = params.get("path", "access_log_Aug95");


		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(numCores);

		// http://flink.apache.org/docs/latest/apis/streaming/index.html

		SourceFunction<LogEntry> logEntrySource = new LogEntrySource(logPath);

		DataStream<LogEntry> logEntries = env.addSource(logEntrySource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEntry>(Time.seconds(60)) {

            @Override
            public long extractTimestamp(LogEntry logEntry) {
                return logEntry.timestamp.getMillis();
            }
        });

		DataStream<Tuple3<Long, String, Long>> bytesPerMonthAndClient = logEntries
				.keyBy((LogEntry e) -> e.client)
				.timeWindow(Time.days(1)) // assuming that all months have 31 days
				.process(new AddBytes());

		DataStream<Tuple3<Long, String, Long>> maxBytesPerDayAndClient = bytesPerMonthAndClient
                .timeWindowAll(Time.days(1)) // assuming that all months have 31 days
                .maxBy(2);

        maxBytesPerDayAndClient.print("Client that requested the most byte resources per day (days in ms, client, transferred bytes) : ").setParallelism(1);

		// execute program
		env.execute("Flink Assignment");
	}

	public static class AddBytes extends ProcessWindowFunction<LogEntry, Tuple3<Long, String, Long>, String, TimeWindow> {
		@Override
		public void process(String key, Context context, Iterable<LogEntry> logEntries, Collector<Tuple3<Long, String, Long>> out) {
			long totalBytes = 0;
			for (LogEntry e : logEntries) {
				totalBytes += e.bytesTransferred;
			}

			out.collect(new Tuple3<>(context.window().getEnd(), key, totalBytes));
		}
	}


}
