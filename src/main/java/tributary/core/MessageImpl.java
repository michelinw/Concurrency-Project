package tributary.core;


import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.UUID;
import tributary.api.Message;
import tributary.api.serialization.Deserializer;
import tributary.api.serialization.Serializer;

public class MessageImpl implements Message {
/**
     *
     */
    private String id;

    /**
     *
     */
    private String payloadType;

    /**
     *
     */
    private String key;

    /**
     *
     */
    private String content;

    /**
     *
     */
    private String datetime;

    /**
     *
     */
    private String serializerPackageName = "tributary.api.serialization";

    /**
     *
     * @param payloadType
     * @param partitionKey
     * @param content
     */
    public MessageImpl(String payloadType, String partitionKey, Object content) {
        this.id = UUID.randomUUID().toString();
        this.payloadType = payloadType;
        this.key = partitionKey;
        this.datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
        setContent(content);
    }

    /**
     *
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     *
     * @return
     */
    public String getPayloadType() {
        return payloadType;
    }

    /**
     *
     * @return
     */
    public String getKey() {
        return key;
    }

    /**
     *
     * @return
     */
    public String getDatetime() {
        return datetime;
    }

    @Override
    public Object getContent() {
        try {
            String serilizedClassName = this.serializerPackageName + "." + payloadType + "Deserializer";
            Object obj = Class.forName(serilizedClassName).getConstructor().newInstance();
            byte[] decodedBytes = Base64.getDecoder().decode(this.content.getBytes());
            return ((Deserializer) obj).deserialize(decodedBytes);

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return "";
    }

    @Override
    public void setContent(Object value) {
        try {
            String serilizedClassName = this.serializerPackageName + "." + payloadType + "Serializer";
            Object obj = Class.forName(serilizedClassName).getConstructor().newInstance();
            byte[] encodedBytes = Base64.getEncoder().encode(((Serializer) obj).serialize(value));
            this.content = new String(encodedBytes);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public final String toString() {
        return "Message [datetime=" + datetime + ", id=" + id + ", payloadType=" + payloadType
            + ", key=" + key + ", value=" + this.getContent().toString() + "]";
    }
}
