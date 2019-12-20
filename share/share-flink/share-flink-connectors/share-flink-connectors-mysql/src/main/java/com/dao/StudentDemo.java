package com.dao;

public class StudentDemo {
    private int stuid;
    private String stuname;
    private String stuaddr;
    private String stusex;

    public int getStuid() {
        return stuid;
    }

    public void setStuid(int stuid) {
        this.stuid = stuid;
    }

    public String getStuname() {
        return stuname;
    }

    public void setStuname(String stuname) {
        this.stuname = stuname;
    }

    public String getStuaddr() {
        return stuaddr;
    }

    public void setStuaddr(String stuaddr) {
        this.stuaddr = stuaddr;
    }

    public String getStusex() {
        return stusex;
    }

    public void setStusex(String stusex) {
        this.stusex = stusex;
    }

    public StudentDemo(int stuid, String stuname, String stuaddr, String stusex) {
        this.stuid = stuid;
        this.stuname = stuname;
        this.stuaddr = stuaddr;
        this.stusex = stusex;
    }

}
