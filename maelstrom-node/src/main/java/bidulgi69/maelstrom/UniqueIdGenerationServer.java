package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class UniqueIdGenerationServer extends Server {

    private final AtomicLong idGenerator;

    public UniqueIdGenerationServer() {
        super();
        this.idGenerator = new AtomicLong(0L);
    }

    public void run() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                Message request = JsonUtil.readValue(line, Message.class);
                ObjectNode body = (ObjectNode) request.getBody();
                String type = body.get("type").asText();
                log("Handling " + request);

                if ("init".equals(type)) {
                    handleInit(request);
                } else if ("generate".equals(type)) {
                    handleGenerate(request);
                }
            }
        } catch (IOException e) {
            throw Error.crash(e.getMessage());
        }
    }

    private void handleGenerate(Message request) {
        ObjectNode body = request.getBody().deepCopy();
        body.put("type", "generate_ok");
        body.put("in_reply_to", body.get("msg_id").asLong());

        long tag = (nodeId.hashCode() & 0xFL) << 60;
        long sequence = idGenerator.getAndIncrement() & ((1L << 60) -1);
        long id = tag | sequence;
        body.put("id", id);

        send(request.getDest(), request.getSrc(), body);
    }
}
