package cn.ljs.temporaltablefunctionjoin.model;
import lombok.Data;

import java.io.Serializable;

@Data
public class CityInfo implements Serializable {
    private Integer cityId;
    private String cityName;
    private Long ts;
}