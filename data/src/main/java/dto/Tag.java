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


    public int compareTo(Tag other) {
        if(truncatedName.toString().length() == other.truncatedName.toString().length())
            return truncatedName.toString().compareTo(other.truncatedName.toString());
        return truncatedName.toString().length() < other.truncatedName.toString().length() ? 1 : 0;
    }

    public String toString(){
        return tagId + ". " + originalName;
    }
}
