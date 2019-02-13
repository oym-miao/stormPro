package com.jesson.session.dao;

import com.jesson.session.WebLogConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 22:40
 * @Description:
 */
public class HBaseConnection {
    private static final HBaseConnection INSTANCE = new HBaseConnection();
    private static Configuration configuration;
    private static Connection connection;

    private HBaseConnection() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", WebLogConstants.zkConnect);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     *
     * 功能描述: 获取table
     *
     * @param:
     * @return:
     * @auther: jesson
     * @date: 2018/8/29 下午10:41
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }
    /**
     *
     * 功能描述: 关闭连接
     *
     * @param:
     * @return:
     * @auther: jesson
     * @date: 2018/8/29 下午10:41
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}
