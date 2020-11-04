package dto;

import java.util.List;

public class Tag {
    public int tagId;
    public String originalName;
    public List<String> truncatedName;
    public int clusterId;

    // 不要删除！！
    public Tag() {
    }

    public void setTagId(int id){
        this.tagId = id;
    }

    public void setOriginalName(String name){
        this.originalName = name;
    }

    public void setTruncatedName(List<String> name){
        this.truncatedName = name;
    }

    public void setClusterId(int id){
        this.clusterId = id;
    }


    public int compareTo(Tag o) {
        if(originalName.equals(o.originalName))
            return 0;
        return originalName.compareTo(o.originalName);
    }

    public String toString(){
        return tagId + ". " + originalName;
    }
}
