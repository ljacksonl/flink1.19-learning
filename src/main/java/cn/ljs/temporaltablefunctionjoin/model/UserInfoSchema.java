package cn.ljs.temporaltablefunctionjoin.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class UserInfoSchema implements DeserializationSchema<UserInfo> {

    @Override
    public UserInfo deserialize(byte[] message) throws IOException {
        String jsonStr = new String(message, StandardCharsets.UTF_8);
        UserInfo data = JSON.parseObject(jsonStr, new TypeReference<UserInfo>() {});
        return data;
    }

    @Override
    public boolean isEndOfStream(UserInfo nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserInfo> getProducedType() {
        return TypeInformation.of(new TypeHint<UserInfo>() {
        });
    }
}