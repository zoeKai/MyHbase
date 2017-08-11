package org.myhbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;


public class HbaseConnnection {
    private final static Log log = LogFactory.getLog(HbaseConnnection.class);
    private static Object lockObj = new Object();
    private static HConnection connection = null;
    public static HConnection getInstance(Configuration config){
        if (connection == null) {
            synchronized (lockObj) {
                if (connection == null) {
                    try {
                        connection = HConnectionManager.createConnection(config);
                    } catch (Exception e) {
                        log.error("getConnection error",e);
                    }
                }
            }
        }
        return connection;
    }
}
