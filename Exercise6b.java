package es.upm.cloud.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.util.Iterator;


public class Exercise6b {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Tuple3<Integer, Long, Double>> mapStream = env.
                readTextFile(params.get("input")).
                map(new MapFunction<String, Tuple3<Integer, Long, Double>>() {
                    public Tuple3<Integer, Long, Double> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        return Tuple3.of(Integer.parseInt(fieldArray[0]),
                        Long.parseLong(fieldArray[1]), Double.parseDouble(fieldArray[2]));
                    }
                });

        DataStream<Tuple3<Integer, Long, Double>> output = mapStream.keyBy(event -> event.f0)
            .window(GlobalWindows.create()) // Use global window instead of count window and specify a trigger and evictor
            .trigger(CountTrigger.of(3)) // Specifies when the reduce function has to be executed
            .evictor(CountEvictor.of(3)) // Specifies how the event have to be removed. Removes the three oldest events
            .reduce(new ReduceFunction<Tuple3<Integer, Long, Double>>() {
                @Override
                public Tuple3<Integer, Long, Double> reduce(Tuple3<Integer, Long, Double> value1,
                                                            Tuple3<Integer, Long, Double> value2) {
                    return value1.f2 > value2.f2 ? value1 : value2;
                }
            });


        if (params.has("output")) {
            output.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            output.print();
        }

        env.execute("Exercise6b");
    }

}
