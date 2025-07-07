package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static ObjectNode createObjectNode() {
        return mapper.createObjectNode();
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
