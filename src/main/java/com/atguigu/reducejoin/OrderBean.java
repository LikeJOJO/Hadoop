package com.atguigu.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private String pid;
    private String pname;
    private int amount;

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    /**
     * 先按照pid排序 ，pid相等再按照pname降序 排列
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
//        return this.pid.compareTo(o.pid);
        int i = this.pid.compareTo(o.pid);
        if (i == 0) {
            return o.pname.compareTo(this.pname);
        } else {
            return i;
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeUTF(pname);
        out.writeInt(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.pname = in.readUTF();
        this.amount = in.readInt();

    }
}
