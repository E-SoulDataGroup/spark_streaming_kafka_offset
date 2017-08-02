package streamingtest;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Description: 数据库连接池类
 * @author dinglq
 */
public class ConnectPool {
    private static Logger log = Logger.getLogger(ConnectPool.class);
    private static BasicDataSource bs = null;

    /**
     * 创建数据源
     * @return
     */
    public static BasicDataSource getDataSource() throws Exception{
        if(bs==null){
            bs = new BasicDataSource();
            bs.setDriverClassName("com.mysql.jdbc.Driver");
            bs.setUrl("jdbc:mysql://xxx.xxx.xxx.xx:3306/spark_stream");
            bs.setUsername("spark_stream");
            bs.setPassword("12345");
            bs.setMaxTotal(50);
            bs.setInitialSize(3);
            bs.setMinIdle(3);
            bs.setMaxIdle(10);
            bs.setMaxWaitMillis(2*10000);
            bs.setRemoveAbandonedTimeout(180);
            bs.setRemoveAbandonedOnBorrow(true);
            bs.setRemoveAbandonedOnMaintenance(true);
            bs.setTestOnReturn(true);
            bs.setTestOnBorrow(true);
        }
        return bs;
    }

    /**
     * 释放数据源
     */
    public static void shutDownDataSource() throws Exception{
        if(bs!=null){
            bs.close();
        }
    }

    /**
     * 获取数据库连接
     * @return
     */
    public static Connection getConnection(){
        Connection con=null;
        try {
            if(bs!=null){
                con=bs.getConnection();
            }else{
                con=getDataSource().getConnection();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return con;
    }

    /**
     * 关闭连接
     */
    public static void closeCon(PreparedStatement ps,Connection con){
        if(ps!=null){
            try {
                ps.close();
            } catch (Exception e) {
                log.error("预编译SQL语句对象PreparedStatement关闭异常！"+e.getMessage(), e);
            }
        }
        if(con!=null){
            try {
                con.close();
            } catch (Exception e) {
                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
            }
        }
    }
}
