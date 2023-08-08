
package tributary.api;

/**
 *
 * @author Michael Wang
 */
public interface Message {
    /**
     *
     * @param content
     */
    public void setContent(Object content);

    /**
     *
     * @return content of the message
     */
    public Object getContent();

    /**
     *
     * @return id of the message
     */
    public String getId();

    /**
     *
     * @return payload type
     */
    public String getPayloadType();

    /**
     *
     * @return key of the message
     */
    public String getKey();

    /**
     *
     * @return datetime of the message
     */
    public String getDatetime();
}
