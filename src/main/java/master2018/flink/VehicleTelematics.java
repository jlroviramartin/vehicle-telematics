package master2018.flink;

import events.events.PrincipalEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * master2018.flink.VehicleTelematics /host/Temp/vehicle-data.csv /host/Temp/
 */
public class VehicleTelematics {

    /**
     * Speed limit used for calculating fines.
     */
    private static final int SPEED_LIMIT = 90;

    /**
     *
     */
    private static final int PARALLELISM = 10;

    /**
     * If @{true} the execution is a test.
     */
    private static final boolean TESTING = true;

    /**
     * If @{true} removes comments and empty lines from the input file.
     */
    private static final boolean REMOVE_COMMENTS = true;

    /**
     * File for debug log.
     */
    private final static String DEBUG_LOG_FILE = "C:\\Temp\\flink\\debug.log";

    /**
     * Log.
     */
    private final static Logger LOG = Logger.getLogger(VehicleTelematics.class.getName());

    /**
     * Initialize the java log at runtime.
     */
    private static void initializeDebugLogger() {
        if (TESTING) {
            try {
                System.setProperty("java.util.logging.SimpleFormatter.format",
                        "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");

                String filePattern = DEBUG_LOG_FILE;
                int limit = 1000 * 1000; // 1 Mb
                int numLogFiles = 3;
                FileHandler handler = new FileHandler(filePattern, limit, numLogFiles);
                handler.setFormatter(new SimpleFormatter());

                LOG.setLevel(Level.ALL);
                LOG.addHandler(handler);
            } catch (IOException | SecurityException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Close the java log at runtime.
     */
    private static void closeDebugLogger() {
        if (TESTING) {
            for (Handler handler : LOG.getHandlers()) {
                handler.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        initializeDebugLogger();

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
            inputFile = Paths.get("C:\\Temp\\flink\\traffic-3xways").toString();
            outputPath = Paths.get("C:\\Temp\\flink").toString();
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
        if (REMOVE_COMMENTS) {
            stream = stream
                    .filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String t) throws Exception {
                            return !t.startsWith("#") && !t.isEmpty();
                        }
                    });
        }

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


        SingleOutputStreamOperator speedFines = SpeedReporter.analyze(toTuples);
        speedFines.writeAsCsv(Paths.get(outputPath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE);


        // NOt yet implemented
        //SingleOutputStreamOperator avgspeedfines = AverageSpeedReporter.analyze(toTuples);
        //avgspeedfines.writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE);


        LOG.info("Executing");

        env.execute("Vehicle telematics");

        LOG.info("End");

        closeDebugLogger();
    }
}
