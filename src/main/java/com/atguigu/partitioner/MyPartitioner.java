package com.atguigu.partitioner;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {
    /**
     * 根据手机号开头返回0-4数字
     *
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String head = text.toString().substring(0, 3);

        int result = 0;

        switch (head) {
            case "136":
                break;
            case "137":
                result = 1;
                break;
            case "138":
                result = 2;
                break;
            case "139":
                result = 3;
                break;
            default:
                result = 10;
                break;
        }

        return result;
    }
}
