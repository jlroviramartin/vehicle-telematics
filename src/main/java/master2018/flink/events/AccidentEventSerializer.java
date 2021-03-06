package master2018.flink.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AccidentEventSerializer extends Serializer<AccidentEvent> {

    @Override
    public void write(Kryo kryo, Output output, AccidentEvent t) {
        output.write(t.getTime1());
        output.write(t.getTime2());
        output.write(t.getVid());
        output.write(t.getHighway());
        output.write(t.getSegment());
        output.write(t.getDirection());
        output.write(t.getPosition());
    }

    @Override
    public AccidentEvent read(Kryo kryo, Input input, Class<AccidentEvent> type) {
        return new AccidentEvent(
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readByte(),
                input.readInt());
    }
}
