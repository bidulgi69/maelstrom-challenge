package bidulgi69.maelstrom;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class EchoServer extends Server {

  public EchoServer() {
    super();
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
        } else if ("echo".equals(type)) {
          handleEcho(request);
        }
      }
    } catch (IOException e) {
      throw Error.crash(e.getMessage());
    }
  }

  private void handleEcho(Message request) {
    ObjectNode body = request.getBody().deepCopy();
    body.put("type", "echo_ok");
    body.put("in_reply_to", body.get("msg_id").asLong());

    send(request.getDest(), request.getSrc(), body);
  }
}
