package es.upm.cloud.flink;

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

public class Exercise5 {
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

        KeyedStream<Tuple3<Integer, Long, Double>, Integer> keyedStream = mapStream.keyBy(event -> event.f0);
        
        DataStream<Tuple3<Integer, Long, Double>> outputMin = keyedStream.min(2);
        DataStream<Tuple3<Integer, Long, Double>> outputMax = keyedStream.max(2);
        DataStream<Tuple3<Integer, Long, Double>> outputSum = keyedStream.sum(2);

        if (params.has("output")) {
            outputMin.writeAsCsv(params.get("output").replace(".csv","min.csv"), FileSystem.WriteMode.OVERWRITE);
            outputMax.writeAsCsv(params.get("output").replace(".csv","max.csv"), FileSystem.WriteMode.OVERWRITE);
            outputSum.writeAsCsv(params.get("output").replace(".csv","sum.csv"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            outputMin.print();
            outputMax.print();
            outputSum.print();
        }

        env.execute("Exercise5");
    }

}
