package es.upm.cloud.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Exercise3 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text;
        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<String,Long,Double>> mapStream = text.
                flatMap(new FlatMapFunction<String, Tuple3<String,Long,Double>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple3<String,Long,Double>> collector) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple3<String,Long,Double> out1 = new Tuple3(fieldArray[0], 
                        Long.parseLong(fieldArray[1]), Double.parseDouble(fieldArray[2]));                        
                        collector.collect(out1);
                        Tuple3<String,Long,Double> out2 = new Tuple3(fieldArray[0]+"-F", 
                        Long.parseLong(fieldArray[1]), (Double.parseDouble(fieldArray[2]) * 9 / 5) + 32);
                        collector.collect(out2);
                    }
                });

        if (params.has("output")) {
            mapStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("Exercise3");
    }

}
