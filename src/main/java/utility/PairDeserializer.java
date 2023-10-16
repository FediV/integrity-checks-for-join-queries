package utility;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;

/**
 * The PairDeserializer class.
 * Custom Apache Commons' Pair deserializer for the CustomJsonParser.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class PairDeserializer extends JsonDeserializer<Pair<?,?>> {
    @Override
    public Pair<?,?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        p.nextValue();
        String left = p.getText();
        p.nextToken();
        p.nextToken();
        String right = p.getText();
        p.nextToken();
        return Pair.of(left, right);
    }
}
