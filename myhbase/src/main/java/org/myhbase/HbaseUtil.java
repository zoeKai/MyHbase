package org.myhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase 工具类
 *
 * @author hanzengliang
 */
public class HbaseUtil {

    private static Configuration config = HBaseConfiguration.create();
    private final static Log log = LogFactory.getLog(HbaseUtil.class);

    public static HTableInterface getTable(String tableName) {
        HTableInterface table = null;
        try {
            table = HbaseConnnection.getInstance(config).getTable(tableName);

        } catch (IOException e) {
            log.error(e);
        }
        return table;
    }


    public static void deleteTable(String tableName,String rows,String family) throws Exception {
        // Instantiating HTable class
        HTable table = new HTable(config, tableName);

        // Instantiating Delete class
        Delete delete = new Delete(Bytes.toBytes(rows));
//        delete.deleteColumn(Bytes.toBytes("au"), Bytes.toBytes("TP99"));
//        delete.deleteColumn(Bytes.toBytes("au"), Bytes.toBytes("dataTime"));
        delete.deleteFamily(Bytes.toBytes(family));

        // deleting the data
        table.delete(delete);

        // closing the HTable object
        table.close();
        System.out.println("data deleted....."+rows);
    }

    /**
     * 批量删除数据
     * @param tableName
     * @param rows
     * @param family
     * @throws Exception
     */
    public static void deleteRows(String tableName,List<String> rows,String family) throws Exception {
        // Instantiating HTable class
        HTable table = new HTable(config, tableName);

        // Instantiating Delete class
        List<Delete> delList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            delList.add(delete);
            delete.deleteFamily(Bytes.toBytes(family));
        }
        // deleting the data
        table.delete(delList);

        // closing the HTable object
        table.close();
        System.out.println("data deleted.....");
    }

    public static void close() {
        try {
            HbaseConnnection.getInstance(config).close();
        } catch (IOException e) {
            log.error(e);
        } finally {
            try {
                HbaseConnnection.getInstance(config).close();
            } catch (IOException e) {
                log.error(e);
            }
        }
    }

}
