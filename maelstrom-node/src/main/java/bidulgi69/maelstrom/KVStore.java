package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

// A KVStore represents a Maelstrom KV service.
public class KVStore {
    final Server node;
    final String service;

    public KVStore(Server node, String service) {
        this.node = node;
        this.service = service;
    }

    // Reads the given key
    public CompletableFuture<JsonNode> read(String k) {
        ObjectNode payload = JsonUtil.createObjectNode();
        payload.put("type", "read");
        payload.put("key", k);
        return node.rpc(service, payload)
            .thenApply((res) -> {
                if (res == null) {
                    throw Error.crash("Invalid response from server");
                }
                return res.get("value");
            });
    }

    // Reads the given key, returning a default if not found
    public CompletableFuture<JsonNode> read(String k, JsonNode defaultValue) {
        return read(k).exceptionally((e) -> {
            // Unwrap completionexceptions
            if (e instanceof CompletionException) {
                e = e.getCause();
            }
            if (e instanceof Error err) {
                if (err.code == 20) {
                    return defaultValue;
                }
            }
            throw new RuntimeException(e);
        });
    }

    // Reads the given key, retrying not-found errors
    public CompletableFuture<JsonNode> readUntilFound(String k) {
        return read(k).exceptionally((e) -> {
            if (e instanceof CompletionException) {
                e = e.getCause();
            }
            if (e instanceof Error err) {
                if (err.code == 20) {
                    return readUntilFound(k).join();
                }
            }
            throw new RuntimeException(e);
        });
    }

    // Writes the given key
    public CompletableFuture<JsonNode> write(String k, JsonNode v) {
        ObjectNode payload = JsonUtil.createObjectNode();
        payload.put("type", "write");
        payload.put("key", k);
        payload.set("value", v);
        return node.rpc(service, payload);
    }

    // Compare-and-sets the given key from `from` to `to`. Creates key if it doesn't
    // exist.
    public CompletableFuture<JsonNode> cas(String k, JsonNode from, JsonNode to) {
        ObjectNode payload = JsonUtil.createObjectNode();
        payload.put("type", "cas");
        payload.put("key", k);
        payload.set("from", from);
        payload.set("to", to);
        payload.put("create_if_not_exists", true);
        return node.rpc(service, payload);
    }
}

