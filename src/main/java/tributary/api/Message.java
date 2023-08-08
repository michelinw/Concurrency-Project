
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
     * @return
     */
    public Object getContent();

    public String getId();

    /**
     *
     * @return
     */
    public String getPayloadType();

    /**
     *
     * @return
     */
    public String getKey();

    /**
     *
     * @return
     */
    public String getDatetime();
}
