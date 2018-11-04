package master2018.flink;

import java.nio.file.Files;
import java.nio.file.Paths;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.ParsePrincipalEventMapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This class is only used for extracting the specific data based on a vehicle from the stream.
 * <p>
 * Command line: master2018.flink.FilterData (source file) (output file) (vid)
 * <p>
 * Example: master2018.flink.FilterData /srv/flink/traffic-3xways /srv/flink/out/28677.csv 28677
 * <p>
 */
public class FilterData {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            throw new IllegalArgumentException("Three parameters are needed");
        }

        String inputFile = args[0];
        if (inputFile == null || !Files.exists(Paths.get(inputFile)) || !Files.isRegularFile(Paths.get(inputFile))) {
            throw new IllegalArgumentException("Argument 1 must be an existing file");
        }

        String outputFile = args[1];
        if (outputFile == null || !Files.exists(Paths.get(outputFile).getParent())) {
            throw new IllegalArgumentException("Argument 2 must be a file in an existing directory");
        }

        int vid;
        try {
            vid = Integer.parseUnsignedInt(args[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument 3 must be an VID");
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> stream = env.readTextFile(inputFile);

        // Evaluates the tuples.
        SingleOutputStreamOperator<PrincipalEvent> toTuples = stream
                .map(new ParsePrincipalEventMapFunction());

        FilterDataPerVehicle.analyze(toTuples, vid)
                .writeAsCsv(outputFile, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Filter data");
    }
}
