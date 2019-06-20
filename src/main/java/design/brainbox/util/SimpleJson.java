package design.brainbox.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class SimpleJson
{
    private final Map<String, Object> data;

    public SimpleJson(Map<String, Object> data)
    {
        this.data = data;
    }

    public static SimpleJson parse(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>(){});
            return new SimpleJson(data);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Object getRaw(String key)
    {
        return data.get(key);
    }

    public String getString(String key) {
        return data.get(key).toString();
    }

    public String getString(String key, String defaultValue) {
        if (this.getString(key) == null) {
            return defaultValue;
        }
        return this.getString(key);
    }

    public Number getNumber(String key) {
        Object number = data.get(key);
        return (Number) number;
    }

    @Override
    public String toString()
    {
        return String.format("[SimpleJson %s]", data.toString());
    }
}
