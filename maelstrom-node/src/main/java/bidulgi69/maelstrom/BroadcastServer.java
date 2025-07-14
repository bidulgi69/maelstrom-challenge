package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class BroadcastServer extends Server {

    private final ExecutorService executor;
    private final int threshold;
    private final long timeout;
    private long lastFlushMs;
    private final LinkedBlockingQueue<Long> queue;
    private final ScheduledExecutorService scheduler;

    private final Set<Long> messages;
    private volatile List<String> neighbors;
    private final Map<Long, CompletableFuture<Void>> futures;

    public BroadcastServer(int threads, int threshold, long timeout) {
        super();
        this.executor = Executors.newFixedThreadPool(threads);
        this.threshold = threshold;
        this.timeout = timeout;
        this.queue = new LinkedBlockingQueue<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.lastFlushMs = System.currentTimeMillis();
        scheduler.scheduleAtFixedRate(this::schedule, 0, 100, TimeUnit.MILLISECONDS);

        this.messages = ConcurrentHashMap.newKeySet();
        // topology RPC를 통해 인접 노드 정보를 구할 수 있음
        this.neighbors = new ArrayList<>();
        this.futures = new ConcurrentHashMap<>();
    }

    private void flush() {
        List<Long> target = new ArrayList<>(threshold);
        queue.drainTo(target, threshold);

        ObjectNode gossip = JsonUtil.createObjectNode();
        gossip.put("type", "broadcast");
        gossip.put("batch", true);
        gossip.set("messages", JsonUtil.longsToJson(target));
        neighbors.forEach(adj -> broadcastGossip(adj, gossip));
    }

    private void schedule() {
        long now = System.currentTimeMillis();
        if (queue.size() >= threshold || now - lastFlushMs > timeout) {
            flush();
            this.lastFlushMs = System.currentTimeMillis();
        }
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
        long msgId = body.get("msg_id").asLong();
        boolean batch = Optional.ofNullable(body.get("batch")).map(JsonNode::asBoolean).orElse(false);
        List<Long> messageIds;
        if (batch) {
            ArrayNode an = (ArrayNode) body.get("messages");
            messageIds = new ArrayList<>(an.size());
            for (JsonNode message : an) {
                messageIds.add(message.asLong());
            }
        } else {
            long message = body.get("message").asLong();
            messageIds = Collections.singletonList(message);
        }

        for (long message : messageIds) {
            if (!messages.contains(message)) {
                messages.add(message);
                queue.offer(message);
            }
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "broadcast_ok");
        resp.put("in_reply_to", msgId);
        send(request.getDest(), request.getSrc(), resp);
    }

    private void handleRead(Message request) {
        ObjectNode body = request.getBody().deepCopy();
        body.put("type", "read_ok");
        body.put("in_reply_to", body.get("msg_id").asLong());
        body.set("messages", JsonUtil.longsToJson(messages));
        send(request.getDest(), request.getSrc(), body);
    }

    private void handleTopology(Message request) {
        ObjectNode body = (ObjectNode) request.getBody();
        ObjectNode topology = (ObjectNode) body.get("topology");
        JsonNode adjacent = topology.get(nodeId);
        if (adjacent.isArray()) {
            List<String> newMembership = new ArrayList<>();
            for (JsonNode adj : adjacent) {
                String adjNode = adj.asText();
                newMembership.add(adjNode);
                // anti-entropy
                for (long messageId : messages) {
                    ObjectNode payload = JsonUtil.createObjectNode();
                    payload.put("type", "broadcast");
                    payload.put("message", messageId);
                    broadcastGossip(adjNode, payload);
                }
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

        CompletableFuture<Void> future = futures.remove(inReplyTo);
        if (future != null) {
            future.complete(null);
        }
    }

    @Override
    public void cleanup() {
        if (!queue.isEmpty()) {
            flush();
        }
        this.scheduler.shutdown();
        super.cleanup();
    }
}
