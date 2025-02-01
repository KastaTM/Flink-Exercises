package es.upm.cloud.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Exercise1 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.readTextFile(params.get("input"));

        if (params.has("output")) {
            text.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
          System.out.println("Printing result to stdout. Use --output to specify output path.");
          text.print();
        }

          env.execute("SourceSink");
    }

}
