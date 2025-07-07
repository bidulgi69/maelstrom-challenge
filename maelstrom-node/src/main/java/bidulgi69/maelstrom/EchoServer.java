package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class EchoServer {

  private final BufferedReader in;
  private final ObjectMapper mapper;

  private String nodeId;
  private List<String> nodeIds;

  public EchoServer() {
    this.in = new BufferedReader(new InputStreamReader(System.in));
    this.mapper = new ObjectMapper();

    this.nodeIds = new ArrayList<>();
  }

  public void run() {
    try {
      String line;
      while ((line = in.readLine()) != null) {
        Message request = mapper.readValue(line, Message.class);
        ObjectNode body = (ObjectNode) request.getBody();
        String type = body.get("type").asText();
        log("Handling " + request);

        if ("init".equals(type)) {
          handleInit(request);
        } else if ("echo".equals(type)) {
          handleEcho(request);
        }
      }
    } catch (IOException e) {
      throw Error.crash(e.getMessage());
    }
  }

  private void handleInit(Message request) throws IOException {
    ObjectNode body = (ObjectNode) request.getBody();
    this.nodeId = body.get("node_id").asText();
    JsonNode topology = body.get("node_ids");
    if (topology.isArray()) {
      for (JsonNode value : topology) {
        nodeIds.add(value.asText());
      }
    }

    ObjectNode responseBody = mapper.createObjectNode();
    responseBody.put("in_reply_to", body.get("msg_id").asLong());
    responseBody.put("type", "init_ok");
    send(request.getSrc(), responseBody);
  }

  private void handleEcho(Message request) throws IOException {
    ObjectNode body = request.getBody().deepCopy();
    body.put("type", "echo_ok");
    body.put("in_reply_to", body.get("msg_id").asLong());

    send(request.getSrc(), body);
  }

  public void send(String dest, ObjectNode body) throws IOException {
    Message message = new Message(nodeId, dest, body);
    log("Sending " + message);

    ObjectNode envelope = toJson(message);
    System.out.println(mapper.writeValueAsString(envelope));
    System.out.flush();
  }

  public void cleanup() {
    try {
      mapper.clearCaches();
      in.close();
    } catch (IOException ignored) {}
  }

  private void log(String message) {
    System.err.println(message);
    System.err.flush();
  }

  private ObjectNode toJson(Message message) {
    ObjectNode json = mapper.createObjectNode();
    json.put("src", message.getSrc());
    json.put("dest", message.getDest());
    json.set("body", message.getBody());
    return json;
  }
}
