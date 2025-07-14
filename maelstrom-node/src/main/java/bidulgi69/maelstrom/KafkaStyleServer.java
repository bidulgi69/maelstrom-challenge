package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class KafkaStyleServer extends Server {

    private final String leaderNode;
    private final State state;
    private final ExecutorService executor;
    // state-machine
    private final KVStore kvStore;
    // logs
    private final ConcurrentMap<String, ConcurrentNavigableMap<Integer, KafkaLog>> logs;
    private final ConcurrentMap<String, Integer> lastCommittedOffsets;

    private final ScheduledExecutorService scheduler;

    public KafkaStyleServer(String leaderNode, int threads) {
        super();
        this.leaderNode = leaderNode;
        this.state = new State();
        this.executor = Executors.newFixedThreadPool(threads);
        this.kvStore = new KVStore(this, "lin-kv");
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        this.logs = new ConcurrentHashMap<>();
        this.lastCommittedOffsets = new ConcurrentHashMap<>();
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
                    switch (type) {
                        case "init" -> handleInit(message);
                        case "send" -> handleSend(message);
                        case "poll" -> handlePoll(message);
                        case "commit_offsets" -> handleCommitOffsets(message);
                        case "list_committed_offsets" -> handleListCommittedOffsets(message);
                        case "replicate" -> handleReplicate(message);
                        default -> handleReply(message);
                    }
                });
            }
        } catch (IOException e) {
            throw Error.crash(e.getMessage());
        }
    }

    @Override
    void handleInit(Message message) {
        super.handleInit(message);
        if (nodeId.equals(leaderNode)) {
            scheduler.scheduleAtFixedRate(this::replicate, 0, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void handleSend(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        if (!nodeId.equals(leaderNode)) {
            rpc(leaderNode, body)
                .thenCompose(r -> {
                    ObjectNode resp = JsonUtil.createObjectNode();
                    resp.put("type", "send_ok");
                    resp.put("in_reply_to", body.get("msg_id").asLong());
                    resp.put("offset", r.get("body").get("offset").asLong());
                    send(message.getDest(), message.getSrc(), resp);
                    return null;
                });
            return;
        }

        String key = body.get("key").asText();
        long msg = body.get("msg").asLong();
        int offset = state.nextOffset();

        KafkaLog log = new KafkaLog(key, msg, offset);
        logs.computeIfAbsent(key, k -> new ConcurrentSkipListMap<>()).put(offset, log);

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "send_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        resp.put("offset", offset);
        send(message.getDest(), message.getSrc(), resp);
    }

    private void handlePoll(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        ObjectNode offsets = (ObjectNode) body.get("offsets");

        Iterator<String> it = offsets.fieldNames();
        ObjectNode msgs = JsonUtil.createObjectNode();
        while (it.hasNext()) {
            String k = it.next();
            int offset = offsets.get(k).asInt();

            ArrayNode array = JsonUtil.createArrayNode();
            logs.getOrDefault(k, new ConcurrentSkipListMap<>())
                .tailMap(offset, true)
                .forEach((_offset, v) -> {
                    ArrayNode pair = JsonUtil.createArrayNode();
                    pair.add(v.offset());
                    pair.add(v.msg());
                    array.add(pair);
                });
            msgs.set(k, array);
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "poll_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        resp.set("msgs", msgs);
        send(message.getDest(), message.getSrc(), resp);
    }

    private void handleCommitOffsets(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        if (!nodeId.equals(leaderNode)) {
            rpc(leaderNode, body)
                .thenCompose(lr -> {
                    ObjectNode resp = JsonUtil.createObjectNode();
                    resp.put("type", "commit_offsets_ok");
                    resp.put("in_reply_to", body.get("msg_id").asLong());
                    send(message.getDest(), message.getSrc(), resp);
                    return null;
                });
            return;
        }

        ObjectNode offsets = (ObjectNode) body.get("offsets");
        Iterator<String> it = offsets.fieldNames();
        while (it.hasNext()) {
            final String key = it.next();
            int commitUpToOffset = offsets.get(key).asInt();

            logs.getOrDefault(key, new ConcurrentSkipListMap<>())
                .headMap(commitUpToOffset, true)
                .forEach((offset, log) -> {
                    // commit
                    kvStore.write(log.key(), JsonUtil.convertValue(log.msg()));
                    lastCommittedOffsets.merge(key, offset, Integer::max);
                });
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "commit_offsets_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        send(message.getDest(), message.getSrc(), resp);
    }

    private void handleListCommittedOffsets(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        ArrayNode keys = (ArrayNode) body.get("keys");

        ObjectNode offsets = JsonUtil.createObjectNode();
        for (JsonNode key : keys) {
            String k = key.asText();
            offsets.put(k, lastCommittedOffsets.getOrDefault(k, 0));
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "list_committed_offsets_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        resp.set("offsets", offsets);
        send(message.getDest(), message.getSrc(), resp);
    }

    private void handleReply(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        long inReplyTo = body.get("in_reply_to").asLong();
        CompletableFuture<JsonNode> f = futures.get(inReplyTo);
        if (f != null) {
            f.complete(body);
        }
    }

    // leader -> follower
    private void replicate() {
        for (String node : nodeIds) {
            if (nodeId.equals(node)) continue;
            ObjectNode replication = JsonUtil.createObjectNode();
            ArrayNode array = JsonUtil.createArrayNode();
            Map<String, Integer> d = new HashMap<>();

            logs.forEach((k, v) -> {
                int nextIndex = state.nextIndex
                    .getOrDefault(node, new ConcurrentSkipListMap<>())
                    .getOrDefault(k, 0);

                // [nextIndex, lastIndex]
                v.tailMap(nextIndex, true)
                    .forEach((offset, log) -> {
                        ObjectNode _log = JsonUtil.createObjectNode();
                        _log.put("key", log.key());
                        _log.put("msg", log.msg());
                        _log.put("offset", offset);
                        array.add(_log);
                        d.merge(k, offset, Integer::max);
                    });
            });

            ObjectNode offsets = JsonUtil.createObjectNode();
            lastCommittedOffsets.forEach(offsets::put);

            replication.set("logs", array);
            replication.set("committed_offsets", offsets);

            // handle ack
            rpc(node, replication)
                .thenCompose(ack -> {
                    String type = ack.get("type").asText();
                    if ("replicate_ok".equals(type)) {
                        d.forEach((k, v) -> {
                            state.nextIndex.get(node).merge(k, v, Integer::max);
                        });
                    }
                    return null;
                });
        }
    }

    private void handleReplicate(Message message) {
        ObjectNode body = (ObjectNode) message.getBody();
        ArrayNode array = (ArrayNode) body.get("logs");
        ObjectNode offsets = (ObjectNode) body.get("committed_offsets");

        // replicate
        for (JsonNode json : array) {
            KafkaLog log = new KafkaLog(json);
            logs.computeIfAbsent(log.key(), k -> new ConcurrentSkipListMap<>()).put(log.offset(), log);
        }
        Iterator<String> it = offsets.fieldNames();
        while (it.hasNext()) {
            String k = it.next();
            int commitOffset = offsets.get(k).asInt();
            lastCommittedOffsets.put(k, commitOffset);
        }

        ObjectNode resp = JsonUtil.createObjectNode();
        resp.put("type", "replicate_ok");
        resp.put("in_reply_to", body.get("msg_id").asLong());
        send(message.getDest(), message.getSrc(), resp);
    }


    // counter(2-RTT)
    private final String gCounterKeyName = "g-counter";
    private final int BATCH_SIZE = 1;
    private int next = 0;
    private int until = -1;

    private synchronized int nextOffset() {
        if (next > until) {
            return refill().join();
        } else {
            return next++;
        }
    }

    private CompletableFuture<Integer> refill() {
        return kvStore.readUntilFound(gCounterKeyName)
            .thenCompose(value -> {
                int preallocate = value.asInt() + BATCH_SIZE;
                return kvStore.cas(gCounterKeyName, value, JsonUtil.convertValue(preallocate))
                    .thenCompose(resp -> {
                        String type = resp.get("type").asText();
                        if ("cas_ok".equals(type)) {
                            next = value.asInt() + 1;
                            until = preallocate;
                            return CompletableFuture.completedFuture(next++);
                        } else {
                            return refill();
                        }
                    });
            });
    }

    @Override
    public void cleanup() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        super.cleanup();
    }
}
