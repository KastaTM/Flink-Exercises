package es.upm.cloud.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.Duration;

public class Exercise7 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1L);

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

        DataStream<Tuple3<Integer, Long, Double>> output = temperatureStream.keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) 
                .aggregate(new AverageAggregate());

        if (params.has("output")) {
            output.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            output.print();
        }

        env.execute("Exercise7");
    }

    // AggregateFunction implementation to calculate de average
    private static class AverageAggregate implements AggregateFunction<Tuple3<Integer, Long, Double>, Tuple4<Integer, Long, Double, Integer>, Tuple3<Integer, Long, Double>> {

        // Initilize the accumulator
        @Override
        public Tuple4<Integer, Long, Double, Integer> createAccumulator() {
            return Tuple4.of(0, 0L, 0.0, 0);
        }
        // Add value to the accumulator
        @Override
        public Tuple4<Integer, Long, Double, Integer> add(Tuple3<Integer, Long, Double> value, Tuple4<Integer, Long, Double, Integer> acc) {
            return Tuple4.of(value.f0, value.f1, acc.f2+value.f2, acc.f3+1);
        }
        // Calculate de average when the window is trigger
        @Override
        public Tuple3<Integer, Long, Double> getResult(Tuple4<Integer, Long, Double, Integer> acc) {
            return Tuple3.of(acc.f0, acc.f1, acc.f2/acc.f3) ;
        }
        // Combine two accumulators (paralelized windows)
        @Override
        public Tuple4<Integer, Long, Double, Integer> merge(Tuple4<Integer, Long, Double, Integer> a, Tuple4<Integer, Long, Double, Integer> b) {
            return Tuple4.of(a.f0, a.f1, a.f2+b.f2, a.f3+b.f3);
        }
    }

}
