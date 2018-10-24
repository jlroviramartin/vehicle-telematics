package master2018.flink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;
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

    public static void main_1(String[] args) throws Exception {

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
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> toTuples = stream
                .map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] split = in.split(",");

                        if (split.length < 8) {
                            throw new Exception("This line cannot be splitted: " + in);
                        }
                        int time = Integer.parseInt(split[0].trim());
                        int vid = Integer.parseInt(split[1].trim());
                        int spd = Integer.parseInt(split[2].trim());
                        int xway = Integer.parseInt(split[3].trim());
                        int lane = Integer.parseInt(split[4].trim());
                        int dir = Integer.parseInt(split[5].trim());
                        int seg = Integer.parseInt(split[6].trim());
                        int pos = Integer.parseInt(split[7].trim());

                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                                time, vid, spd, xway, lane, dir, seg, pos
                        );
                        return out;
                    }
                });

        // Evaluates the speed fines.
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedFines = toTuples
                .map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> t) throws Exception {

                        int time = t.f0;
                        int vid = t.f1;
                        int spd = t.f2;
                        int xway = t.f3;
                        //int lane = t.f4;
                        int dir = t.f5;
                        int seg = t.f6;
                        //int pos = t.f7;
                        return new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(
                                time, vid, xway, seg, dir, spd);
                    }
                })
                .filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> t) throws Exception {
                        int spd = (int) t.f5;
                        return spd > SPEED_LIMIT;
                    }
                });

        speedFines.writeAsCsv(Paths.get(outputPath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE);

        LOG.info("Executing");

        env.execute("Vehicle telematics");

        LOG.info("End");

        closeDebugLogger();
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
            outputPath = Paths.get("C:\\Temp\\flink\\").toString();
        }

        LOG.info("inputFile " + inputFile);
        LOG.info("outputPath " + outputPath);

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> toTuples = stream
                .map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] split = in.split(",");

                        if (split.length < 8) {
                            throw new Exception("This line cannot be splitted: " + in);
                        }
                        int time = Integer.parseInt(split[0].trim());
                        int vid = Integer.parseInt(split[1].trim());
                        int spd = Integer.parseInt(split[2].trim());
                        int xway = Integer.parseInt(split[3].trim());
                        int lane = Integer.parseInt(split[4].trim());
                        int dir = Integer.parseInt(split[5].trim());
                        int seg = Integer.parseInt(split[6].trim());
                        int pos = Integer.parseInt(split[7].trim());

                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                                time, vid, spd, xway, lane, dir, seg, pos
                        );
                        return out;
                    }
                });

        // Evaluates the speed fines.
        SingleOutputStreamOperator<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> avgspeedfines = toTuples
                .filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> t) throws Exception {
                        int segment = (int) t.f6;
                        return segment >= 52 && segment <= 56;
                    }
                })
                .map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {

                        return new Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                                in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, in.f6, in.f7, 1
                        );
                    }
                });

        // vid (1), xway (3), dir (5)

        /*SingleOutputStreamOperator<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> reduce = avgspeedfines
                .keyBy(1, 3, 5)
                .reduce(
                        new ReduceFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value1,
                                                                                                                          Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value2)
                            throws Exception {
                        return new Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                                value1.f0, value1.f1, value1.f2, value1.f3, value1.f4, value1.f5, value1.f6, value1.f7, value1.f8 + value2.f8);
                    }
                });*/

        avgspeedfines.writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE);

        LOG.info("Executing");

        env.execute("Vehicle telematics");

        LOG.info("End");

        closeDebugLogger();
    }
}
