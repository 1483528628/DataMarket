package com.itheima.canal_demo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.itheima.protobuf.DemoModel;

public class ProtoBufDemo {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        DemoModel.User.Builder builder = DemoModel.User.newBuilder();
        builder.setId(1);
        builder.setName("张三");
        builder.setSex("男");
        byte[] bytes = builder.build().toByteArray();
        System.out.println("- - -protobuf- - -");
        for (byte aByte : bytes) {
            System.out.println(aByte);
        }

        System.out.println("----");
        DemoModel.User user = DemoModel.User.parseFrom(bytes);
        System.out.println(user.toString());
    }
}
