package com.atguigu.comparable2;

import com.atguigu.comparable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ComparePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
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
                result = 4;
                break;
        }

        return result;
    }
}
