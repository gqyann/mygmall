package com.atguigu.app;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanclClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();

            canalConnector.subscribe("gmall.*");

            //获取数据
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> messageEntries = message.getEntries();

            //判断是否有数据
            if (messageEntries.size() <= 0) {
                System.out.println("没有数据，等待一会儿...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry messageEntry : messageEntries) {
                    CanalEntry.EntryType entryType = messageEntry.getEntryType();
                    //获取表名
                    String tableName = messageEntry.getHeader().getTableName();

                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        ByteString storeValue = messageEntry.getStoreValue();
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }
        }

    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if (tableName.equals("order_info") && CanalEntry.EventType.INSERT.equals(eventType)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toString());

                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER, jsonObject.toString());

            }
        }
    }
}
