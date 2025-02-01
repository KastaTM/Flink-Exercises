package es.upm.cloud.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Exercise2 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text;
        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<Integer,Long,Double>> mapStream = text.
                map(new MapFunction<String, Tuple3<Integer,Long,Double>>() {
                    public Tuple3<Integer,Long,Double> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple3<Integer,Long,Double> out = new Tuple3(Integer.parseInt(fieldArray[0]), 
                        Long.parseLong(fieldArray[1]), Double.parseDouble(fieldArray[2]));
                        return out;
                    }
                }).filter(new FilterFunction<Tuple3<Integer,Long,Double>>() {
            @Override
            public boolean filter(Tuple3<Integer,Long,Double> tuple) throws Exception {
                return tuple.f0.equals(1);
            }
        });

        if (params.has("output")) {
            mapStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("Exercise2");
    }

}
