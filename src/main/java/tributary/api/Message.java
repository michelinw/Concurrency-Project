
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
}
