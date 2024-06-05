package cn.ljs.temporaltablefunctionjoin.model;
import lombok.Data;

import java.io.Serializable;

@Data
public class UserInfo implements Serializable {
    private String userName;
    private Integer cityId;
    private Long ts;
}