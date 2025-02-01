package es.upm.cloud.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Exercise10 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                .where(temp -> temp.f0) // Join by sendor Id in tempereature stream
                .equalTo(press -> press.f0) // Join by sendor Id in pressure stream
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10 seconds window
                .apply(new JoinFunction<Tuple3<Integer, Long, Double>, Tuple3<Integer, Long, Double>, Tuple4<Integer, Long, Double, Double>>() {
                    @Override
                    public Tuple4<Integer, Long, Double, Double> join(Tuple3<Integer, Long, Double> temp, Tuple3<Integer, Long, Double> press) {
                        return Tuple4.of(temp.f0, temp.f1, temp.f2, press.f2); // (sensorId, timestamp, temperature, pressure)
                    }
                });


        if (params.has("output")) {
            joinedStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            joinedStream.print();
        }

        env.execute("Exercise8");
    }

}