package master2018.flink;

import java.nio.file.Files;
import java.nio.file.Paths;
import master2018.flink.events.AccidentEvent;
import master2018.flink.events.AccidentEventSerializer;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedEventSerializer;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.AverageSpeedTempEventSerializer;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.events.PrincipalEventSerializer;
import master2018.flink.events.SpeedEvent;
import master2018.flink.events.SpeedEventSerializer;
import master2018.flink.functions.ParsePrincipalEventMapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This class is the main class for vehicle telematics.
 * <p>
 * Command line: master2018.flink.VehicleTelematics (source file) (output path)
 * <p>
 * Example: master2018.flink.VehicleTelematics /srv/flink/traffic-3xways /srv/flink/out/
 * <p>
 */
public final class VehicleTelematics {

    public static void main(String[] args) throws Exception {

        String inputFile, outputPath;
        if (args.length < 2) {
            throw new IllegalArgumentException("Two parameters are needed");
        }

        inputFile = args[0];
        if (inputFile == null || !Files.exists(Paths.get(inputFile)) || !Files.isRegularFile(Paths.get(inputFile))) {
            throw new IllegalArgumentException("Argument 1 must be an existing file");
        }

        outputPath = args[1];
        if (outputPath == null || !Files.exists(Paths.get(outputPath)) || !Files.isDirectory(Paths.get(outputPath))) {
            throw new IllegalArgumentException("Argument 2 must be an existing directory");
        }

        // Get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // triggers flushing only when the output buffer is full thus maximizing throughput
        env.setBufferTimeout(-1);

        // Enable object reuse to increase pereformance
        env.getConfig().enableObjectReuse();

        // Kryo serializers
        env.getConfig().enableForceKryo();
        env.getConfig().registerTypeWithKryoSerializer(AccidentEvent.class, AccidentEventSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(AverageSpeedEvent.class, AverageSpeedEventSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(AverageSpeedTempEvent.class, AverageSpeedTempEventSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(PrincipalEvent.class, PrincipalEventSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(SpeedEvent.class, SpeedEventSerializer.class);

        // Ensure that the paralelism is 1 by default
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get input data by reading a text file
        DataStream<String> stream = env.readTextFile(inputFile);

        // Evaluates the tuples.
        SingleOutputStreamOperator<PrincipalEvent> toTuples = stream
                .map(new ParsePrincipalEventMapFunction());

        // Speed reporter
        SingleOutputStreamOperator speedReporter = SpeedReporter.analyze(toTuples);
        speedReporter
                .writeAsCsv(Paths.get(outputPath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // Average speed reporter
        DataStream avgspeedfines = AverageSpeedReporter.analyze(toTuples);
        avgspeedfines
                .writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // Accident reporter
        SingleOutputStreamOperator accidentReporter = AccidentReporter.analyze(toTuples);
        accidentReporter
                .writeAsCsv(Paths.get(outputPath, "accidents.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Vehicle telematics");
    }
}
