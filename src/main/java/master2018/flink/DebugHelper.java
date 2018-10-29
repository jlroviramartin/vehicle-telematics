/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static master2018.flink.VehicleTelematics.DEBUG_LOG_FILE;

/**
 *
 * @author joseluis
 */
public class DebugHelper {

    private final static Logger LOG = Logger.getLogger(DebugHelper.class.getName());
    private static FileHandler handler;

    /**
     * Initialize the java log at runtime.
     */
    public static void initializeDebugLogger() {
        if (VehicleTelematics.TESTING) {
            try {
                System.setProperty("java.util.logging.SimpleFormatter.format",
                                   "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");

                String filePattern = DEBUG_LOG_FILE;
                int limit = 1000 * 1000; // 1 Mb
                int numLogFiles = 1;

                handler = new FileHandler(filePattern, limit, numLogFiles);
                handler.setFormatter(new SimpleFormatter());

                Logger global = Logger.getGlobal();
                global.setLevel(Level.ALL);
                global.addHandler(handler);
            } catch (IOException | SecurityException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Close the java log at runtime.
     */
    public static void closeDebugLogger() {
        if (VehicleTelematics.TESTING) {
            Logger global = Logger.getGlobal();
            global.removeHandler(handler);
            handler.close();
            handler = null;
        }
    }
}
