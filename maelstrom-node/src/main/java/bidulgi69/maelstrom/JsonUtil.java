package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }

    public static ArrayNode createArrayNode() {
        return mapper.createArrayNode();
    }

    public static <T> JsonNode convertValue(T value) {
        return mapper.convertValue(value, JsonNode.class);
    }

    public static ArrayNode longsToJson(Iterable<Long> longs) {
        ArrayNode arrayNode = mapper.createArrayNode();
        for (long l : longs) {
            arrayNode.add(l);
        }
        return arrayNode;
    }

    public static <T> T readValue(String source, Class<T> clazz) throws IOException {
        return mapper.readValue(source, clazz);
    }

    public static String writeValueAsString(JsonNode value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (IOException e) {
            throw Error.crash(e.getMessage());
        }
    }
}
