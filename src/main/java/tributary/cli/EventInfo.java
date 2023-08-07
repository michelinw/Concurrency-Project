package tributary.cli;

import org.json.JSONObject;

/**
 *
 * @author Michael Wang
 */
public class EventInfo {
    private Object value;
    private String key;

    public static final EventInfo parseEvent(String eventFileName) {
        EventInfo ei = new EventInfo();
        JSONObject jsonEvent = new JSONObject(eventFileName);
        ei.key = jsonEvent.getString("KEY");
        ei.value = jsonEvent.getString(eventFileName);
        return ei;
    }

    public Object getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }
}
