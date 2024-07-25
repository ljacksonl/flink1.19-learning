import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Main {
    public static void main(String[] args) throws JsonProcessingException {
        String yamlData = "key: value\nanotherKey: anotherValue";

        // 使用 SnakeYAML 将 YAML 数据转换为 JsonNode
        ObjectMapper yamlMapper = new YAMLMapper();
        JsonNode jsonNode = yamlMapper.readTree(yamlData);

        // 使用 Jackson 将 JsonNode 转换为 JSON 字符串
        ObjectMapper jsonMapper = new ObjectMapper();
        String jsonData = jsonMapper.writeValueAsString(jsonNode);

        System.out.println("JSON data: " + jsonData);

        String jsonData1 = "{\"key\": \"value\", \"anotherKey\": \"anotherValue\"}";

        // 使用 TypeSafe Config 读取 JSON 数据
        Config config = ConfigFactory.parseString(jsonData1);
        String keyValue = config.getString("key");
        String anotherKeyValue = config.getString("anotherKey");

        System.out.println("Key value: " + keyValue);
        System.out.println("Another key value: " + anotherKeyValue);
    }
}
