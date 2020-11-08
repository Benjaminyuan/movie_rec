package hbase;

import java.util.Map;

public class Status {
    private boolean ok;
    private String info;
    private Map<String,String> resultMap;
    public Status(boolean ok, String info) {
        this.ok = ok;
        this.info = info;
    }

    public boolean isOk() {
        return ok;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public String getInfo() {
        return info;
    }

    public void setResultMap(Map<String, String> resultMap) {
        this.resultMap = resultMap;
    }

    public Map<String, String> getResultMap() {
        return resultMap;
    }
}
