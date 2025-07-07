package bidulgi69.maelstrom;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {
    private String src;
    private String dest;
    private JsonNode body;

    public Message() {}

    public Message(String src, String dest, JsonNode body) {
        this.src = src;
        this.dest = dest;
        this.body = body;
    }

    public Message(ObjectNode json) {
        this.src = json.get("src").asText();
        this.dest = json.get("dest").asText();
        this.body = json.get("body");
    }

    public String getSrc() {
        return src;
    }

    public String getDest() {
        return dest;
    }

    public JsonNode getBody() {
        return body;
    }

    @Override
    public String toString() {
        return String.format("{src: %s, dest: %s, body: %s}", src, dest, body);
    }
}
