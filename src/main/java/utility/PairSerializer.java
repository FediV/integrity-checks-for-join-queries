package utility;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;

/**
 * The PairSerializer class.
 * Custom Apache Commons' Pair serializer for the CustomJsonParser.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@SuppressWarnings("rawtypes")
public final class PairSerializer extends JsonSerializer<Pair> {
    @Override
    public void serialize(Pair value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField("left", value.getLeft());
        gen.writeObjectField("right", value.getRight());
        gen.writeEndObject();
    }
}
