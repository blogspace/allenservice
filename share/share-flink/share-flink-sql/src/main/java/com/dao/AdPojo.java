package com.dao;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdPojo {
    //.pojoType(AdPojo.class, "channel", "subject", "refer", "reg", "ord", "pv", "uv");;
    private String channel;
    private String subject;
    private String refer;
    private String reg;
    private String ord;
    private String pv;
    private String uv;


}
