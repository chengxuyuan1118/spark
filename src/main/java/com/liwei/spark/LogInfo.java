package com.liwei.spark;

import java.io.Serializable;

public class LogInfo implements Serializable,Comparable<LogInfo> {

    private static final long serialVersionUID = 5749943279909593929L;
    private long timeStamp;
    private String phoneNo;
    private long down;
    private long up;


    LogInfo() {
    }

    public LogInfo(long down, String phoneNo, long timeStamp, long up) {
        this.timeStamp = timeStamp;
        this.phoneNo = phoneNo;
        this.down = down;
        this.up = up;
    }
    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getPhoneNo() {
        return phoneNo;
    }

    public void setPhoneNo(String phoneNo) {
        this.phoneNo = phoneNo;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    @Override
    public int compareTo(LogInfo o) {
        return 0;
    }
}
