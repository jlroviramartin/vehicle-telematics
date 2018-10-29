package master2018.flink;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.*;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * master2018.flink.VehicleTelematics /host/flink/vehicle-data.csv /host/flink/
 */
public class VehicleTelematics {

    /**
     * If @{true} the execution is a test.
     */
    public static final boolean TESTING = true;

    /**
     * If @{true} removes comments and empty lines from the input file.
     */
    private static final boolean REMOVE_COMMENTS = true;

    /**
     * File for debug log.
     */
    public final static String DEBUG_BASEPATH_DOCKER = "/host/flink";
    public final static String DEBUG_BASEPATH_WIN = "C:\\Temp\\flink";
    public final static String DEBUG_BASEPATH = DEBUG_BASEPATH_DOCKER;
    public final static String DEBUG_LOG_FILE = Paths.get(DEBUG_BASEPATH, "debug.log").toString();
    public final static String DEBUG_INPUTFILE = Paths.get(DEBUG_BASEPATH, "traffic-3xways.sample").toString();
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
                throw new IllegalArgumentException("Argument 1 must be an existing file 3)");
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

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(PARALLELISM);

        // get input data by connecting to the socket
        DataStream<String> stream = env.readTextFile(inputFile);

        // This is used to remove the comments of the input file. It is only useful with tests.
        /*if (REMOVE_COMMENTS) {
            stream = stream
                    .filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String t) throws Exception {
                            return !t.startsWith("#") && !t.isEmpty();
                        }
                    });
        }*/

        // Evaluates the tuples.
        SingleOutputStreamOperator<PrincipalEvent> toTuples = stream
                .map(new MapFunction<String, PrincipalEvent>() {

                    @Override
                    public PrincipalEvent map(String in) throws Exception {

                        String[] split = in.split(",");

                        if (split.length < 8) {
                            throw new Exception("This line cannot be splitted: " + in);
                        }

                        PrincipalEvent principalEvent = new PrincipalEvent(Integer.parseInt(split[0].trim()),
                                                                           Integer.parseInt(split[1].trim()),
                                                                           Integer.parseInt(split[2].trim()),
                                                                           Integer.parseInt(split[3].trim()),
                                                                           Integer.parseInt(split[4].trim()),
                                                                           Integer.parseInt(split[5].trim()),
                                                                           Integer.parseInt(split[6].trim()),
                                                                           Integer.parseInt(split[7].trim()));
                        return principalEvent;
                    }
                });

        // 1st test
        /*
        SingleOutputStreamOperator speedFines = SpeedReporter.analyze(toTuples);
        speedFines.writeAsCsv(Paths.get(outputPath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
         */
        // 2nd test
        // NOt yet implemented
        SingleOutputStreamOperator avgspeedfines = AverageSpeedReporter.analyze(toTuples);
        /*avgspeedfines.writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);*/

        LOG.info("Executing the jobs");

        env.execute("Vehicle telematics");

        LOG.info("End");

        DebugHelper.closeDebugLogger();
    }
}
