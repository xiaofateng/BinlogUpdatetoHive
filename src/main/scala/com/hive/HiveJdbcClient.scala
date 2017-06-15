package com.hive

/**
  * Created by xiaoft on 2017/1/12.
  * 使用Hive jdbc连接hive2,进行增删改操作.
  */

import java.sql.{Connection, DriverManager, SQLException, Statement}

object HiveJdbcClient {
  private var driverName: String = "org.apache.hive.jdbc.HiveDriver"

  /**
    *    * @param args
    *    * @throws SQLException
    *    */
  @throws(classOf[SQLException])
  def main(args: Array[String]) {
    try {
      Class.forName(driverName)
    }
    catch {
      case e: ClassNotFoundException => {
        e.printStackTrace
        System.exit(1)
      }
    }
    //jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;initFile=<file>;sess_var_list?hive_conf_list#hive_var_list
    val con: Connection = DriverManager.getConnection("jdbc:hive2://ip:10000/tmp" +
      ";" +
      ";hive.support.concurrency=true;hive.enforce.bucketing=true;hive.exec.dynamic.partition.mode=nonstrict;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager" +
      "?hive.support.concurrency=true;hive.enforce.bucketing=true;hive.exec.dynamic.partition.mode=nonstrict;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager" +
      "#hive.support.concurrency=true;hive.enforce.bucketing=true;hive.exec.dynamic.partition.mode=nonstrict;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"
      , "hadoop", "hadoop")
    val stmt: Statement = con.createStatement
    val tableName: String = "xdual"
    //stmt.execute("drop table if exists " + tableName)
    //stmt.execute("create table " + tableName + " (id int, x timestamp)")
    //var sql: String = "show tables '" + tableName + "'"
    //System.out.println("Running: " + sql)
    //var res: ResultSet = stmt.executeQuery(sql)
    //if (res.next) {
     // System.out.println(res.getString(1))
    //}


    var sql = "select * from tmp.xdual"
    System.out.println("Running: " + sql)
    var res = stmt.executeQuery(sql)
    while (res.next) {
      System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2))
    }

    var insertSql: String = "insert into table tmp.xdual values(200090,\"2016-09-26 16:37:22\")()()()()()"
    System.out.println("Running: " + insertSql)
    res = stmt.executeQuery(insertSql)
  }
}

