package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Server {
    final BufferedReader in;
    final AtomicLong idGenerator;
    final Map<Long, CompletableFuture<JsonNode>> futures;

    String nodeId;
    List<String> nodeIds;

    Server() {
        this.in = new BufferedReader(new InputStreamReader(System.in));
        this.idGenerator = new AtomicLong(0L);
        this.futures = new ConcurrentHashMap<>();
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
        body.put("msg_id", createUniqueId());
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

    private long createUniqueId() {
        long tag = (nodeId.hashCode() & 0xFL) << 60;
        long sequence = idGenerator.getAndIncrement() & ((1L << 60) -1);
        return tag | sequence;
    }

    CompletableFuture<JsonNode> rpc(String adj, ObjectNode body) {
        CompletableFuture<JsonNode> future = new CompletableFuture<>();
        send(nodeId, adj, body);
        futures.put(body.get("msg_id").asLong(), future);
        return future;
    }

    void broadcastGossip(String adjacent, ObjectNode body) {
        rpc(adjacent, body)
            .orTimeout(1_000, TimeUnit.MILLISECONDS)
            .exceptionally(e -> {
                log("Retry gossip to " + adjacent + ", msgId: " + body.get("msg_id").asLong());
                broadcastGossip(adjacent, body);
                return null;
            });
    }
}
