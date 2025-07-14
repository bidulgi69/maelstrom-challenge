package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;

public record KafkaLog(
    String key,
    long msg,
    int offset
) implements Comparable<KafkaLog> {

    public KafkaLog(JsonNode json) {
        this(
            json.get("key").asText(),
            json.get("msg").asLong(),
            json.get("offset").asInt()
        );
    }

    @Override
    public int compareTo(KafkaLog o) {
        return Integer.compare(offset, o.offset);
    }
}
