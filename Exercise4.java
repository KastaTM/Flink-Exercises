package es.upm.cloud.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Exercise4 {
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

        DataStream<Tuple3<Integer, Long, Double>> output = mapStream.keyBy(event -> event.f0).reduce(new ReduceFunction<Tuple3<Integer, Long, Double>>() {
            @Override
            public Tuple3<Integer, Long, Double> reduce(Tuple3<Integer, Long, Double> value1, Tuple3<Integer, Long, Double> value2) throws Exception {
                return new Tuple3<Integer, Long, Double>(value2.f0, value1.f1, value1.f2+value2.f2);
            }
        });

        if (params.has("output")) {
            output.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            output.print();
        }

        env.execute("Exercise4");
    }

}
