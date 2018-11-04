package master2018.flink;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.*;
import master2018.flink.events.PrincipalEvent;
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
public class VehicleTelematics {

    /**
     * If @{true} the execution is a test.
     */
    public static final boolean TESTING = true;

    /**
     * File for debug log.
     */
    //public final static String DEBUG_BASEPATH = "C:\\Temp\\flink";
    public final static String DEBUG_BASEPATH = "/srv/flink";
    public final static String DEBUG_LOG_FILE = Paths.get(DEBUG_BASEPATH, "debug.log").toString();
    public final static String DEBUG_INPUTFILE = Paths.get(DEBUG_BASEPATH, "traffic-3xways").toString();
    public final static String DEBUG_OUTPUTPATH = Paths.get(DEBUG_BASEPATH, "out").toString();

    /**
     * Log.
     */
    private final static Logger LOG = Logger.getLogger(VehicleTelematics.class.getName());

    public static void main(String[] args) throws Exception {

        DebugHelper.initializeDebugLogger();

        LOG.info("Begin");

        String inputFile, outputPath;
        if (!TESTING) {
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
        } else {
            inputFile = Paths.get(DEBUG_INPUTFILE).toString();
            outputPath = Paths.get(DEBUG_OUTPUTPATH).toString();
        }

        LOG.info("inputFile " + inputFile);
        LOG.info("outputPath " + outputPath);

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> stream = env.readTextFile(inputFile);

        // Evaluates the tuples.
        SingleOutputStreamOperator<PrincipalEvent> toTuples = stream
                .map(new ParsePrincipalEventMapFunction())
                .setParallelism(1);

        // 1st test
        SingleOutputStreamOperator speedReporter = SpeedReporter.analyze(toTuples);
        speedReporter
                .writeAsCsv(Paths.get(outputPath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // 2nd test
        SingleOutputStreamOperator avgspeedfines = AverageSpeedReporter_4.analyze(toTuples);
        avgspeedfines.writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // 3er test
        SingleOutputStreamOperator accidentReporter = AccidentReporter.analyze(toTuples);
        accidentReporter
                .writeAsCsv(Paths.get(outputPath, "accidents.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        LOG.info("Executing the jobs");

        env.execute("Vehicle telematics");

        LOG.info("End");

        DebugHelper.closeDebugLogger();
    }
}
