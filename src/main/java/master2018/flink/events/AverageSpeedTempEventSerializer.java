package master2018.flink.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class AverageSpeedTempEventSerializer extends Serializer<AverageSpeedTempEvent> {

    @Override
    public void write(Kryo kryo, Output output, AverageSpeedTempEvent t) {
        output.write(t.getTime1());
        output.write(t.getTime2());
        output.write(t.getVid());
        output.write(t.getHighway());
        output.write(t.getDirection());
        output.write(t.getPosition1());
        output.write(t.getPosition2());
        output.write(t.getSegment1());
        output.write(t.getSegment2());
    }

    @Override
    public AverageSpeedTempEvent read(Kryo kryo, Input input, Class<AverageSpeedTempEvent> type) {
        return new AverageSpeedTempEvent(
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readByte());
    }
}
