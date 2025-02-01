package es.upm.cloud.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Exercise10a {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        double temperatureLimit = 75.0;
        double pressureLimit = 1.0;

        WatermarkStrategy<Tuple3<Integer, Long, Double>> watermarkStrategy = WatermarkStrategy
                .<Tuple3<Integer, Long, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.f1);


        DataStream<Tuple3<Integer, Long, Double>> temperatureStream = env.
                readTextFile(params.get("input")).
                map(new MapFunction<String, Tuple3<Integer, Long, Double>>() {
                    public Tuple3<Integer, Long, Double> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        return Tuple3.of(Integer.parseInt(fieldArray[0]),
                        Long.parseLong(fieldArray[1]), Double.parseDouble(fieldArray[2]));
                    }
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Tuple3<Integer, Long, Double>> pressureStream = env.
                readTextFile(params.get("input2")).
                map(new MapFunction<String, Tuple3<Integer, Long, Double>>() {
                    public Tuple3<Integer, Long, Double> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        return Tuple3.of(Integer.parseInt(fieldArray[0]),
                        Long.parseLong(fieldArray[1]), Double.parseDouble(fieldArray[2]));
                    }
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Tuple4<Integer, Long, Double, Double>> joinedStream = temperatureStream
                .join(pressureStream)
                .where(temp -> temp.f0) // Join by sendor Id in tempereature stream. If we want to join by more than one field .where(temp -> Tuple2.of(temp.f0, temp.f1))
                .equalTo(press -> press.f0) // Join by sendor Id in pressure stream.If we want to join by more than one field .where(press -> Tuple2.of(press.f0, press.f1))
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10 seconds window
                .apply(new JoinFunction<Tuple3<Integer, Long, Double>, Tuple3<Integer, Long, Double>, Tuple4<Integer, Long, Double, Double>>() {
                    @Override
                    public Tuple4<Integer, Long, Double, Double> join(Tuple3<Integer, Long, Double> temp, Tuple3<Integer, Long, Double> press) {
                        return Tuple4.of(temp.f0, temp.f1, temp.f2, press.f2); // (sensorId, timestamp, temperature, pressure)
                    }
                });
        
        // Calculate the average of the temperature and pressure and verify the limits
        DataStream<String> alertStream = joinedStream
        .keyBy(value -> value.f0) // Group by sensorId
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10 seconds window
        .aggregate(new AverageAggregate(), new WindowFunction<Tuple3<Double, Double, Long>, String, Integer, TimeWindow>() {
            @Override
            public void apply(Integer key, TimeWindow window, Iterable<Tuple3<Double, Double, Long>> avgResult, Collector<String> out) {
                Tuple3<Double, Double, Long> result = avgResult.iterator().next();
                Double avgTemp = result.f0;
                Double avgPressure = result.f1;
                if (avgTemp > temperatureLimit && avgPressure > pressureLimit) {
                    out.collect("Alert: SensorId " + key + " - Avg Temp: " + avgTemp + ", Avg Presión: " + avgPressure);
                }
            }
        });

        if (params.has("output")) {
            alertStream.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            alertStream.print();
        }

        env.execute("Exercise9");
    }

   
    public static class AverageAggregate implements AggregateFunction<Tuple4<Integer, Long, Double, Double>, Tuple3<Double, Double, Long>, Tuple3<Double, Double, Long>> {


        @Override
        public Tuple3<Double, Double, Long> createAccumulator() {
            return Tuple3.of(0.0, 0.0, 0L); // (sumTemp, sumPressure, counter)
        }

        @Override
        public Tuple3<Double, Double, Long> add(Tuple4<Integer, Long, Double, Double> value, Tuple3<Double, Double, Long> accumulator) {
            return Tuple3.of(accumulator.f0 + value.f2, accumulator.f1 + value.f3, accumulator.f2 + 1);
        }

        @Override
        public Tuple3<Double, Double, Long> getResult(Tuple3<Double, Double, Long> accumulator) {
            return Tuple3.of(accumulator.f0 / accumulator.f2, accumulator.f1 / accumulator.f2, accumulator.f2);
        }

        @Override
        public Tuple3<Double, Double, Long> merge(Tuple3<Double, Double, Long> a, Tuple3<Double, Double, Long> b) {
            return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}