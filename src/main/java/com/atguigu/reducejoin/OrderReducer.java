package com.atguigu.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        ArrayList<OrderBean> list = new ArrayList<>();
//
//        //1. 找到 品牌
//        String pname = "";
//        for (NullWritable value : values) {
//            if (!key.getPname().equals("")) {
//                pname = key.getPname();
//            } else {
//                OrderBean temp = new OrderBean();
//                try {
//                    BeanUtils.copyProperties(temp, key);
//                    list.add(temp);
//                } catch (IllegalAccessException | InvocationTargetException e) {
//                    e.printStackTrace();
//                }
//
//            }
//        }
//
//        //2. 替换品牌并输出
//        for (OrderBean orderBean : list) {
//            orderBean.setPname(pname);
//            context.write(orderBean, NullWritable.get());
//        }


        //1. 找品牌
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();

        String pname = key.getPname();

        //2. 遍历输出
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }

    }
}
