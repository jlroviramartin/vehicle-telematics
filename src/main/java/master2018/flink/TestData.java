/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink;

import java.nio.file.Paths;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.ParsePrincipalEventMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static master2018.flink.VehicleTelematics.DEBUG_INPUTFILE;
import static master2018.flink.VehicleTelematics.DEBUG_OUTPUTPATH;

/**
 * master2018.flink.TestData
 */
public class TestData {

    public static void main(String[] args) throws Exception {

        String inputFile = Paths.get(DEBUG_INPUTFILE).toString();
        String outputPath = Paths.get(DEBUG_OUTPUTPATH).toString();
        String outputFile = Paths.get(outputPath, "TestData.csv").toString();

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> stream = env.readTextFile(inputFile);

        // Evaluates the tuples.
        SingleOutputStreamOperator<PrincipalEvent> toTuples = stream
                .map(new ParsePrincipalEventMapFunction());

        toTuples
                .filter(new FilterFunction<PrincipalEvent>() {
                    @Override
                    public boolean filter(PrincipalEvent value) throws Exception {
                        return value.getSegment() != Utils.getSegment(value.getPosition());
                    }
                })
                .writeAsCsv(outputFile, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Test data");
    }
}
