package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BroadcastServer extends Server {

    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final Set<Long> messages;
    private volatile List<String> neighbors;
    private final AtomicLong idGenerator;
    private final Map<Long, CompletableFuture<Void>> calls;

    public BroadcastServer() {
        super();
        this.messages = ConcurrentHashMap.newKeySet();
        // topology RPC를 통해 인접 노드 정보를 구할 수 있음
        this.neighbors = new ArrayList<>();
        this.idGenerator = new AtomicLong(0L);
        this.calls = new ConcurrentHashMap<>();
    }

    public void run() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                Message message = JsonUtil.readValue(line, Message.class);
                ObjectNode body = (ObjectNode) message.getBody();
                String type = body.get("type").asText();
                log("Handling " + message);

                executor.execute(() -> {
                    if ("broadcast_ok".equals(type)) {
                        handleReply(message);
                    } else {
                        switch (type) {
                            case "init" -> handleInit(message);
                            case "broadcast" -> handleBroadcast(message);
                            case "read" -> handleRead(message);
                            case "topology" -> handleTopology(message);
                        }
                    }
                });
            }
        } catch (IOException e) {
            throw Error.crash(e.getMessage());
        }
    }

    private void handleBroadcast(Message request) {
        ObjectNode body = request.getBody().deepCopy();
        long messageId = body.get("message").asLong();
        if (!messages.contains(messageId)) {
            messages.add(messageId);

            // 충돌을 막기 위해 gossip 전송 시점에 인접 노드를 복사한 뒤 처리
            final List<String> snapshot = new ArrayList<>(neighbors);
            broadcastGossip(snapshot, body);
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "broadcast_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        resp.put("msg_id", body.get("msg_id").asLong());
        send(request.getDest(), request.getSrc(), resp);
    }

    private void handleRead(Message request) {
        ObjectNode body = request.getBody().deepCopy();
        ArrayNode messageIds = JsonUtil.createArrayNode();
        for (Long mId : messages) { // capture
            messageIds.add(mId);
        }
        body.put("type", "read_ok");
        body.put("in_reply_to", body.get("msg_id").asLong());
        body.set("messages", messageIds);
        send(request.getDest(), request.getSrc(), body);
    }

    private void handleTopology(Message request) {
        ObjectNode body = (ObjectNode) request.getBody();
        ObjectNode topology = (ObjectNode) body.get("topology");
        JsonNode adjacent = topology.get(nodeId);
        if (adjacent.isArray()) {
            List<String> newMembership = new ArrayList<>();
            for (JsonNode adj : adjacent) {
                newMembership.add(adj.asText());
            }
            // swap
            this.neighbors = newMembership;
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "topology_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        send(request.getDest(), request.getSrc(), resp);
    }

    private void handleReply(Message reply) {
        ObjectNode body = (ObjectNode) reply.getBody();
        long inReplyTo = Optional.ofNullable(body.get("in_reply_to"))
            .map(JsonNode::asLong)
            .orElse(-1L);

        CompletableFuture<Void> call = calls.remove(inReplyTo);
        if (call != null) {
            call.complete(null);
        }
    }

    private void broadcastGossip(List<String> adjacent, ObjectNode body) {
        for (String adj : adjacent) {
            long msgId = idGenerator.incrementAndGet();
            body.put("msg_id", msgId);
            CompletableFuture<Void> call = gossip(adj, body);
            calls.putIfAbsent(msgId, call);
        }
    }

    private CompletableFuture<Void> gossip(String adj, ObjectNode body) {
        return CompletableFuture.runAsync(() -> send(nodeId, adj, body), executor)
            .orTimeout(1_000, TimeUnit.MILLISECONDS)
            .exceptionally(e -> {
                log("Gossip failed: " + e);
                gossip(adj, body); // retry
                return null;
            });
    }
}
