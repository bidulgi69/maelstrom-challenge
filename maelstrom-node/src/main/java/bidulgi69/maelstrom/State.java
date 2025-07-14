package bidulgi69.maelstrom;

import java.util.HashMap;
import java.util.Map;

public class State {
    int offset;
    Map<String, Map<String, Integer>> nextIndex;

    public State() {
        this.offset = 0;
        this.nextIndex = new HashMap<>();
    }

    public synchronized int nextOffset() {
        return ++offset;
    }
}
