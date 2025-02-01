package es.upm.cloud.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Exercise9a {
    final static OutputTag<TemperatureEvent> lateOutputTag = new OutputTag<TemperatureEvent>("late-events"){};

    public static void main(String[] args) throws Exception {
        // Flink runtime environment configuration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);

        // Socket port configuration
        String hostname = "jobmanager";
        int port = 9999;

        // Connect to socket and receive data as text strings
        DataStream<TemperatureEvent> eventStream = env.socketTextStream(hostname, port).map(new MapFunction<String, TemperatureEvent>() {
            @Override
            public TemperatureEvent map(String value) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(value, TemperatureEvent.class);
            }
        });

        // Assign timestamps and generate watermarks
        WatermarkStrategy<TemperatureEvent> watermarkStrategy = WatermarkStrategy
                .<TemperatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        SingleOutputStreamOperator<TemperatureEvent> timestampedStream = eventStream
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // Group the data by machineId and apply a fixed time window
        SingleOutputStreamOperator<TemperatureAverage> averageTemperature = timestampedStream
                .keyBy(TemperatureEvent::getMachineId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))  // 10 seconds window
                //.allowedLateness(Time.seconds(10)) // Allow 5 seconds late events
                .sideOutputLateData(lateOutputTag) // Send late data to side output
                .process(new AverageTemperatureProcessWindowFunction());

        averageTemperature.writeAsText("/files/average_temperature.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataStream<TemperatureEvent> lateDataStream = averageTemperature.getSideOutput(lateOutputTag);
        lateDataStream.writeAsText("/files/average_temperature_late.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Execute Flink job
        env.execute("Socket Window Query Example");
    }

    // Temperature event type
    public static class TemperatureEvent {
        private Integer machineId;
        private double temperature;
        private long timestamp;

        public TemperatureEvent() {}

        public TemperatureEvent(Integer machineId, double temperature, long timestamp) {
            this.machineId = machineId;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        public Integer getMachineId() {
            return machineId;
        }

        public double getTemperature() {
            return temperature;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "TemperatureEvent{" +
                    "machineId='" + machineId + '\'' +
                    ", temperature=" + temperature +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    // Class for the average temperature result
    public static class TemperatureAverage {
        private Integer machineId;
        private double averageTemperature;
        private long startTs;
        private long endTs;

        public TemperatureAverage(Integer machineId, double averageTemperature, long startTs, long endTs) {
            this.machineId = machineId;
            this.averageTemperature = averageTemperature;
            this.startTs = startTs;
            this.endTs = endTs;
        }

        @Override
        public String toString() {
            return "TemperatureAverage{" +
                    "machineId='" + machineId + '\'' +
                    ", averageTemperature=" + averageTemperature +
                    ", startTS=" + startTs +
                    ", endTS=" + endTs +
                    '}';
        }
    }

    public static class AverageTemperatureProcessWindowFunction extends ProcessWindowFunction<TemperatureEvent, TemperatureAverage, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, Iterable<TemperatureEvent> elements, Collector<TemperatureAverage> out) {
            int count = 0;
            double sum = 0.0;
            // Iterate over the elements to calculate the average
            for (TemperatureEvent element : elements) {
                sum += element.getTemperature();
                count++;
            }
            double average = count > 0 ? (sum / count) : 0;
            out.collect(new TemperatureAverage(key, average, context.window().getStart(), context.window().getEnd()));
        }
    }
}
