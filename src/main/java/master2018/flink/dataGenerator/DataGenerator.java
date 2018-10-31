/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.dataGenerator;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import master2018.flink.events.PrincipalEvent;

/**
 *
 * @author joseluis
 */
public class DataGenerator {

    public final static int SEGMENT_SIZE = 5280;
    public final static int ENTRY_LANE = 0;
    public final static int TRAVEL_1_LANE = 1;
    public final static int TRAVEL_2_LANE = 2;
    public final static int TRAVEL_3_LANE = 3;
    public final static int EXIT_LANE = 4;

    public final static int EAST_DIRECTION = 0; // Increment
    public final static int WEST_DIRECTION = 1; // Decrement

    public final static int TIME_INCREMENT = 30;

    public final static Logger LOG = Logger.getLogger(DataGenerator.class.getName());

    public static void main(String[] args) {
        ArrayList<PrincipalEvent> totalResult = new ArrayList<>();
        totalResult.addAll(buildSampleData1());
        totalResult.addAll(buildSampleData2());
        totalResult.sort((PrincipalEvent o1, PrincipalEvent o2) -> Integer.compare(o1.getTime(), o2.getTime()));

        try (PrintStream stream = new PrintStream("C:\\Temp\\flink\\traffic-3xways.sample")) {
            printData(totalResult, stream);
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } finally {
        }
    }

    /**
     * Sample data 1: Direction 0 (increase) Vehicle 1: OK Vehicle 2: OK It doesn't arrive at 56. Vehicle 3: OK It
     * doesn't start in 53. Vehicle 4: OK It doesn't start in 53 and doesn't arrive at 56. Vehicle 5: FINE!
     *
     * @return
     */
    public static List<PrincipalEvent> buildSampleData1() {
        int highway = 1;
        int direction = EAST_DIRECTION;

        // tramo [52, 56]
        ArrayList<PrincipalEvent> totalResult = new ArrayList<>();

        // Vehicle 1 OK
        {
            int vid = 1;
            int lane = TRAVEL_1_LANE;
            int time = 0;
            int mphSpeed = 80;
            int position1 = getPosition(51, 0);
            int position2 = getPosition(57, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle 2 OK, no llega a 56
        {
            int vid = 2;
            int lane = TRAVEL_2_LANE;
            int time = 305;
            int mphSpeed = 95;
            int position1 = getPosition(51, 0);
            int position2 = getPosition(54, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle 3 OK, no empieza en 53
        {
            int vid = 3;
            int lane = TRAVEL_2_LANE;
            int time = 200;
            int mphSpeed = 95;
            int position1 = getPosition(53, 0);
            int position2 = getPosition(57, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle 4 OK, no empieza en 53 y no llega a 56
        {
            int vid = 4;
            int lane = TRAVEL_2_LANE;
            int time = 200;
            int mphSpeed = 95;
            int position1 = getPosition(53, 0);
            int position2 = getPosition(54, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle 5 MULTA
        {
            int vid = 5;
            int lane = TRAVEL_3_LANE;
            int time = 245;
            int mphSpeed = 92;
            int position1 = getPosition(51, 0);
            int position2 = getPosition(57, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }
        return totalResult;
    }

    /**
     * Sample data 2: Direction 1 (decrease) Vehicle 20: OK Vehicle 21: OK It doesn't start in 56. Vehicle 22: OK It
     * doesn't arrive at 52. Vehicle 23: OK It doesn't start in 56 and doesn't arrive at 52. Vehicle 24: FINE!
     *
     * @return
     */
    public static List<PrincipalEvent> buildSampleData2() {
        int highway = 1;
        int direction = WEST_DIRECTION;

        // tramo [52, 56]
        ArrayList<PrincipalEvent> totalResult = new ArrayList<>();

        // Vehicle OK
        {
            int vid = 20;
            int lane = TRAVEL_1_LANE;
            int time = 0;
            int mphSpeed = 80;
            int position1 = getPosition(57, 0);
            int position2 = getPosition(51, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle OK, no empieza en 56
        {
            int vid = 21;
            int lane = TRAVEL_2_LANE;
            int time = 305;
            int mphSpeed = 95;
            int position1 = getPosition(54, 0);
            int position2 = getPosition(51, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle OK, no llega a 52
        {
            int vid = 22;
            int lane = TRAVEL_2_LANE;
            int time = 200;
            int mphSpeed = 95;
            int position1 = getPosition(57, 0);
            int position2 = getPosition(53, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle OK, no empieza en 56 y no llega a 52
        {
            int vid = 23;
            int lane = TRAVEL_2_LANE;
            int time = 200;
            int mphSpeed = 95;
            int position1 = getPosition(54, 0);
            int position2 = getPosition(53, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }

        // Vehicle MULTA
        {
            int vid = 24;
            int lane = TRAVEL_3_LANE;
            int time = 245;
            int mphSpeed = 92;
            int position1 = getPosition(57, 0);
            int position2 = getPosition(51, 0);
            totalResult.addAll(vehicleFromTo(vid, highway, direction, lane, time, mphSpeed, position1, position2));
        }
        return totalResult;
    }

    /**
     * This method builds a trip at speed {@code mphSpeed} miles/h for the vehicle {@code vid} in the highway
     * {@code highway} in the direction {@code direction} using the lane {@code lane} from the position
     * {@code position1} to the position {@code position2} in meters, starting at {@code time} seconds.
     *
     * @param vid Vehicle.
     * @param highway Highway.
     * @param direction Direction.
     * @param lane Lane.
     * @param time Starting time.
     * @param mphSpeed Speed.
     * @param position1 Start position.
     * @param position2 End position.
     * @return List of events.
     */
    public static List<PrincipalEvent> vehicleFromTo(int vid, int highway, int direction, int lane, int time, int mphSpeed, int position1, int position2) {
        int incPosition = metersAtSpeed(mphSpeed, TIME_INCREMENT);

        System.out.println("Vehicle " + vid + " from " + getSegment(position1) + " to " + getSegment(position2) + " at " + mphSpeed + "mph");
        System.out.println("    It runs " + incPosition + "m every " + TIME_INCREMENT + "s");

        if (direction == EAST_DIRECTION && position1 > position2) {
            throw new InvalidParameterException("position1 > position2 for East direction");
        } else if (direction == WEST_DIRECTION && position1 < position2) {
            throw new InvalidParameterException("position1 < position2 for West direction");
        }

        ArrayList<PrincipalEvent> events = new ArrayList<>();

        PrincipalEvent event = new PrincipalEvent();
        event.setVid(vid);
        event.setHighway(highway);
        event.setDirection(direction);
        event.setSpeed(mphSpeed);

        int position = position1;

        // Entry
        event.setTime(time);
        event.setLane(ENTRY_LANE);
        event.setSegment(getSegment(position));
        event.setPosition(position);

        // Add a copy of the event
        events.add(copy(event));

        if (direction == EAST_DIRECTION) { // increase
            position += incPosition;
            time += TIME_INCREMENT;
            while (position < position2) {

                // Traveling
                event.setTime(time);
                event.setLane(lane);
                event.setSegment(getSegment(position));
                event.setPosition(position);

                // Add a copy of the event
                events.add(copy(event));

                position += incPosition;
                time += TIME_INCREMENT;
            }
        } else { // decrease
            position -= incPosition;
            time += TIME_INCREMENT;
            while (position > position2) {

                // Traveling
                event.setTime(time);
                event.setLane(lane);
                event.setSegment(getSegment(position));
                event.setPosition(position);

                // Add a copy of the event
                events.add(copy(event));

                position -= incPosition;
                time += TIME_INCREMENT;
            }
        }

        // Exit
        event.setTime(time);
        event.setLane(EXIT_LANE);
        event.setSegment(getSegment(position));
        event.setPosition(position);

        // Add a copy of the event
        events.add(copy(event));

        return events;
    }

    /**
     * This method calculates the amount of meters that a vehicle runs in {@code second} seconds at {@code milesPerHour}
     * miles/hour.
     *
     * @param milesPerHour Speed in miles per hour.
     * @param seconds Seconds.
     * @return Meters.
     */
    public static int metersAtSpeed(int milesPerHour, int seconds) {
        return (int) (1609.34 * milesPerHour * seconds / 3600);
    }

    /**
     * This method converts {@code metersPerSecond} m/s to miles per hour.
     *
     * @param metersPerSecond Speed in meter per second.
     * @return Speed in miles per hour.
     */
    public static int getMilesPerHour(int metersPerSecond) {
        return (int) Math.floor(metersPerSecond * 2.23694);
    }

    /**
     * This method converts {@code milesPerHour} miles/h to meters/second.
     *
     * @param milesPerHour Speed in miles per hour.
     * @return Speed in meter per second.
     */
    public static int getMetersPerSecond(int milesPerHour) {
        return (int) Math.floor(milesPerHour / 2.23694);
    }

    /**
     * This method converts {@code miles} miles to meters.
     *
     * @param miles Miles.
     * @return Meters.
     */
    public static int getMeters(int miles) {
        return (int) (miles * 1609.34);
    }

    /**
     * This method converts {@code meters} to miles.
     *
     * @param meters Meters.
     * @return Miles.
     */
    public static int getMiles(int meters) {
        return (int) (meters / 1609.34);
    }

    /**
     * This methods calculates the position in meters for the {@code segment} segment and delta of {@code delta} meters.
     *
     * @param segment Segment.
     * @param delta Delta in meters.
     * @return Position in meters.
     */
    public static int getPosition(int segment, int delta) {
        return (segment * SEGMENT_SIZE) + delta;
    }

    /**
     * This methos calculates the segment for the {@code position} position in meters.
     *
     * @param position Position in meters.
     * @return Segment.
     */
    public static int getSegment(int position) {
        return position / SEGMENT_SIZE;
    }

    public static String toString(PrincipalEvent event) {
        StringBuilder buff = new StringBuilder();
        buff.append(event.getTime());
        buff.append(",");
        buff.append(event.getVid());
        buff.append(",");
        buff.append(event.getSpeed());
        buff.append(",");
        buff.append(event.getHighway());
        buff.append(",");
        buff.append(event.getLane());
        buff.append(",");
        buff.append(event.getDirection());
        buff.append(",");
        buff.append(event.getSegment());
        buff.append(",");
        buff.append(event.getPosition());
        return buff.toString();
    }

    public static PrincipalEvent copy(PrincipalEvent event) {
        return new PrincipalEvent(event.f0, event.f1, event.f2, event.f3, event.f4, event.f5, event.f6, event.f7);
    }

    public static void printData(List<PrincipalEvent> toPrint, PrintStream stream) {
        for (PrincipalEvent event : toPrint) {
            stream.println(toString(event));
        }
    }
}
