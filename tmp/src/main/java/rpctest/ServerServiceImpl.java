package rpctest;

import java.util.HashMap;
import java.util.Map;

public class ServerServiceImpl implements ServerService {

    public String get(Object key) {
        Map<String,Object> map = new HashMap<String, Object>();
        map.put("aaa","111");
        map.put("bbb","222");
        map.put("ccc",new Object());
        Object obj = map.get(key);
        if(null == obj){
            return null;
        }
        if(obj instanceof String){
            return (String)obj;
        }
        return obj.toString();
    }
}
