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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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

		DataStream<Tuple3<Long, String, Long>> maxBytesPerDayAndClient = logEntries
				.keyBy(e -> e.client)
				.timeWindow(Time.days(1))
				.process(new AddBytes())
				.timeWindowAll(Time.days(1))
                .maxBy(2);

        maxBytesPerDayAndClient.print("Client that requested the most byte resources per day (days in ms, client, transferred bytes) : ").setParallelism(1);

        // Most requested resource (total sum)
		SingleOutputStreamOperator<Tuple2<String, Integer>> mostRequestedResource = logEntries
                .flatMap(new ResourceMapper())
                .keyBy(0)
                .timeWindow(Time.hours(10))
                .sum(1)
                .timeWindowAll(Time.hours(10))
                .maxBy(1);

        mostRequestedResource.print("Most requested resource within 10 hours (resource, accesses)");


		// Average number of transferred byter per hour for successful GET requests
		SingleOutputStreamOperator<Tuple2<Long, Float>> avg = logEntries
				.filter(e -> e.httpStatus == 200 && e.httpMethod.equals("GET"))
				.keyBy(e -> e.httpMethod)
				.timeWindow(Time.hours(1))
				.process(new AverageBytesTransferred());

		avg.print("Average number of transferred bytes per hour for GET requests with 200 -> mean, not median! ;): ");

		// Average no. requests per hour
		logEntries
				.timeWindowAll(Time.hours(1))
				.process(new CountRequestsPerWindow())
				.timeWindowAll(Time.days(1))
				.process(new AverageRequestsPerDay())
				.print("Average requests per hour (day, average) : ");

        // execute program
		env.execute("Flink Assignment");
	}

	public static class AverageBytesTransferred extends ProcessWindowFunction<LogEntry, Tuple2<Long, Float>, String, TimeWindow> {

		@Override
		public void process(String key, Context context, Iterable<LogEntry> logEntries, Collector<Tuple2<Long, Float>> out) {
			float sum = 0f;
			float count = 0f;

			for (LogEntry e : logEntries) {
				sum += e.bytesTransferred;
				count++;
			}

			out.collect(new Tuple2<>(context.window().getEnd(), (sum / count)));
		}
	}

	public static class CountRequestsPerWindow extends ProcessAllWindowFunction<LogEntry, Long, TimeWindow> {

		@Override
		public void process(Context context, Iterable<LogEntry> logEntries, Collector<Long> out) {
			long count = 0;

			for (LogEntry e : logEntries) {
				count++;
			}

			out.collect(count);
		}
	}

	public static class AverageRequestsPerDay extends ProcessAllWindowFunction<Long, Tuple2<Long, Long>, TimeWindow> {

		@Override
		public void process(Context context, Iterable<Long> preResults, Collector<Tuple2<Long, Long>> out) {
			long count = 0;
			long sum = 0;

			for (Long r : preResults) {
				count++;
				sum += r;
			}

			out.collect(new Tuple2<>(context.window().getEnd(), sum / count));
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
