package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public abstract class Server {
    final BufferedReader in;

    String nodeId;
    List<String> nodeIds;

    Server() {
        this.in = new BufferedReader(new InputStreamReader(System.in));

        this.nodeIds = new ArrayList<>();
    }

    abstract void run();

    void handleInit(Message request) {
        ObjectNode body = (ObjectNode) request.getBody();
        this.nodeId = body.get("node_id").asText();
        JsonNode topology = body.get("node_ids");
        if (topology.isArray()) {
            for (JsonNode value : topology) {
                nodeIds.add(value.asText());
            }
        }

        ObjectNode responseBody = JsonUtil.createObjectNode();
        responseBody.put("in_reply_to", body.get("msg_id").asLong());
        responseBody.put("type", "init_ok");
        send(request.getDest(), request.getSrc(), responseBody);
    }

    void send(String src, String dest, ObjectNode body) {
        Message message = new Message(src, dest, body);
        log("Sending " + message);

        ObjectNode envelope = toJson(message);
        System.out.println(JsonUtil.writeValueAsString(envelope));
        System.out.flush();
    }

    void cleanup() {
        try {
            in.close();
        } catch (IOException ignored) {}
    }

    void log(String message) {
        System.err.println(message);
        System.err.flush();
    }

    ObjectNode toJson(Message message) {
        ObjectNode json = JsonUtil.createObjectNode();
        json.put("src", message.getSrc());
        json.put("dest", message.getDest());
        json.set("body", message.getBody());
        return json;
    }
}
