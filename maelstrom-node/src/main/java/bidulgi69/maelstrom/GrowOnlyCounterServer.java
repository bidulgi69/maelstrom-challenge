package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

// CRDTs
public class GrowOnlyCounterServer extends Server {

    private final Map<String, Long> counter;
    private final ScheduledExecutorService gossiper;

    public GrowOnlyCounterServer() {
        super();
        this.counter = new ConcurrentHashMap<>();
        this.gossiper = Executors.newSingleThreadScheduledExecutor();
    }

    private void schedule() {
        ObjectNode gossip = JsonUtil.createObjectNode();
        gossip.put("type", "broadcast");
        gossip.put("value", counter.get(nodeId));

        for (String node : nodeIds) {
            if (node.equals(nodeId)) continue;
            broadcastGossip(node, gossip);
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

                switch (type) {
                    case "init" -> handleInit(message);
                    case "add" -> handleAdd(message);
                    case "read" -> handleRead(message);
                    case "broadcast" -> handleBroadcast(message);
                    case "broadcast_ok" -> handleReply(message);
                }
            }
        } catch (IOException e) {
            throw Error.crash(e.getMessage());
        }
    }

    @Override
    void handleInit(Message message) {
        super.handleInit(message);
        for (String nodeId : nodeIds) {
            counter.put(nodeId, 0L);
        }
        // gossip(merge)
        gossiper.scheduleAtFixedRate(this::schedule, 1_000, 1_000, TimeUnit.MILLISECONDS);
    }

    private void handleAdd(Message message) {
        ObjectNode body = message.getBody().deepCopy();
        long delta = body.get("delta").asLong();
        counter.merge(nodeId, delta, Long::sum);

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "add_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        send(message.getDest(), message.getSrc(), resp);
    }

    private void handleRead(Message message) {
        ObjectNode body = message.getBody().deepCopy();
        long value = counter.values().stream().mapToLong(Long::longValue).sum();

        body.put("type", "read_ok");
        body.put("in_reply_to", body.get("msg_id").asLong());
        body.put("value", value);
        send(message.getDest(), message.getSrc(), body);
    }

    private void handleBroadcast(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        String src = message.getSrc();
        long value = body.get("value").asLong();
        // 갱신
        counter.put(src, value);

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "broadcast_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        send(message.getDest(), message.getSrc(), resp);
    }

    private void handleReply(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        long inReplyTo = Optional.ofNullable(body.get("in_reply_to"))
            .map(JsonNode::asLong)
            .orElse(-1L);

        CompletableFuture<?> future = futures.remove(inReplyTo);
        if (future != null) {
            future.complete(null);
        }
    }

    @Override
    public void cleanup() {
        this.gossiper.shutdown();
        super.cleanup();
    }
}
