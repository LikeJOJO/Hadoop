package com.atguigu.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator {

    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    /**
     * 自定义比较器，不再调用默认的compareTO方法，直接比较两个pid
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
