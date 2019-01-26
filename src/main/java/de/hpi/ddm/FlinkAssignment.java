package de.hpi.ddm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


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


		BoundedOutOfOrdernessTimestampExtractor<LogEntry> timestampExtractor = new BoundedOutOfOrdernessTimestampExtractor<LogEntry>(Time.seconds(60)) {
			@Override
			public long extractTimestamp(LogEntry logEntry) {
				return logEntry.timestamp.getMillis();
			}
		};
		DataStream<LogEntry> logEntries = env.addSource(logEntrySource).assignTimestampsAndWatermarks(timestampExtractor);

		// Client that requested the most byte resources per day

		DataStream<Tuple3<Long, String, Long>> bytesPerMonthAndClient = logEntries
				.keyBy(e -> e.client)
				.timeWindow(Time.days(1))
				.process(new AddBytes());

		DataStream<Tuple3<Long, String, Long>> maxBytesPerDayAndClient = bytesPerMonthAndClient
                .timeWindowAll(Time.days(1))
                .maxBy(2);

        //maxBytesPerDayAndClient.print("Client that requested the most byte resources per day (days in ms, client, transferred bytes) : ").setParallelism(1);

        // Most requested resource (total count)
		SingleOutputStreamOperator<Tuple2<String, Integer>> mostRequestedResource = logEntries
                .flatMap(new ResourceMapper())
                .keyBy(0)
                .timeWindow(Time.hours(10))
                .sum(1)
                .timeWindowAll(Time.hours(10))
                .maxBy(1);

        mostRequestedResource.print("Most requested resource within 10 hours (resource, accesses)").setParallelism(1);


		SingleOutputStreamOperator<Tuple3<Long, String, Float>> avg = logEntries
				.filter(e -> e.httpStatus == 200)
				.filter(e -> e.httpMethod.equals("GET"))
				.keyBy(e -> e.httpMethod)
				.timeWindow(Time.hours(1))
				.process(new MyProcessWindowFunction());

				avg.print("Average number of transferred bytes per hour for GET requests with 200 : ").setParallelism(1);

        // execute program
		env.execute("Flink Assignment");
	}

	public static class MyProcessWindowFunction extends ProcessWindowFunction<LogEntry, Tuple3<Long, String, Float>, String, TimeWindow> {

		@Override
		public void process(String key, Context context, Iterable<LogEntry> logEntries, Collector<Tuple3<Long, String, Float>> out) {
			float sum = 0f;
			float count = 0f;

			for (LogEntry e : logEntries) {
				sum += e.bytesTransferred;
				count++;
			}

			out.collect(new Tuple3<>(context.window().getEnd(), key, (sum / count)));
		}
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

    public static final class ResourceMapper implements FlatMapFunction<LogEntry, Tuple2<String, Integer>> {

        @Override
        public void flatMap(LogEntry value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<>(value.resource, 1));
        }
    }

}
