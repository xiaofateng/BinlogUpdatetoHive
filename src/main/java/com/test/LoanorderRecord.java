/**
这是一个借贷关系的类
 */

package com.test;

import com.util.TimeParseUtil;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.io.Text;

import java.sql.Timestamp;

public class LoanorderRecord {


  public final long id;
  public final Text uuid;
  public final Text link_uuid;
  public final int link_length;
    public final int sequence;
    public final Text from_user;
    public final Text to_user;
    public final HiveDecimal price;
    public final Text product_uuid;
  public final Text product_code;
    public final int product_type;
    public final HiveDecimal rate;
  public final Timestamp start_time;
  public final Timestamp end_time;
    public final int order_status;
    public final Timestamp pay_time;
  public final Text debtor;
    public final HiveDecimal principal;
    public final HiveDecimal interest;
    public final Timestamp create_time;
    public final Timestamp update_time;
    public final int record_flag;
    public final Text remark;
    public final Text laundry_uuid;
    public final Text from_laundry_uuid;
    public final Text debt_uuid;
    public final Text debt_link_uuid;
    public final Text from_debt_uuid;
    public final long root_code;
    public final Text proto_version;
    public final Text guarantee;
    public final int lend_out_type;
    public final Timestamp product_create_time;
    public RecordIdentifier rowId;


  public LoanorderRecord(
                        String id,
                        String uuid,
                        String link_uuid,
                        String link_length,
                        String sequence,
                        String from_user,
                        String to_user,
                        String price,
                        String product_uuid,
                        String product_code,
                        String product_type,
                        String rate,
                        String start_time,
                        String end_time,
                        String order_status,
                        String pay_time,
                        String debtor,
                        String principal,
                        String interest,
                        String create_time,
                        String update_time,
                        String record_flag,
                        String remark,
                        String laundry_uuid,
                        String from_laundry_uuid,
                        String debt_uuid,
                        String debt_link_uuid,
                        String from_debt_uuid,
                        String root_code,
                        String proto_version,
                        String guarantee,
                        String lend_out_type,
                        String product_create_time,
                        RecordIdentifier rowId)
  {
    this.id = Long.parseLong(id);
    this.uuid = new Text(uuid);
    this.link_uuid = new Text(link_uuid);
    this.link_length=Integer.parseInt(link_length);
    this.sequence=Integer.parseInt(sequence);
    this.from_user=new Text(from_user);
    this.to_user=new Text(to_user);
    this.product_uuid=new Text(product_uuid);
    this.product_code=new Text(product_code);
    this.product_type=Integer.parseInt(product_type);
    this.price=HiveDecimal.create(price);
    this.rate=HiveDecimal.create(rate);
    this.start_time= TimeParseUtil.parse(start_time);
    this.end_time= TimeParseUtil.parse(end_time);
    this.order_status=Integer.parseInt(order_status);
    this.pay_time= TimeParseUtil.parse(pay_time);
    this.debtor=new Text(debtor);
      this.principal=HiveDecimal.create(principal);
      this.interest=HiveDecimal.create(interest);
      this.create_time= TimeParseUtil.parse(create_time);
      this.update_time= TimeParseUtil.parse(update_time);
      this.record_flag=Integer.parseInt(record_flag);
      this.remark=new Text(remark);
      this.laundry_uuid=new Text(laundry_uuid);
      this.from_laundry_uuid=new Text(from_laundry_uuid);
      this.debt_uuid=new Text(debt_uuid);
      this.debt_link_uuid=new Text(debt_link_uuid);
      this.from_debt_uuid=new Text(from_debt_uuid);
      this.root_code=Long.parseLong(root_code);
      this.proto_version=new Text(proto_version);
      this.guarantee=new Text(guarantee);
      this.lend_out_type=Integer.parseInt(lend_out_type);
      this.product_create_time= TimeParseUtil.parse(product_create_time);
      this.rowId = rowId;
  }



    public LoanorderRecord(
            String id,
            String uuid,
            String link_uuid,
            String link_length,
            String sequence,
            String from_user,
            String to_user,
            String price,
            String product_uuid,
            String product_code,
            String product_type,
            String rate,
            String start_time,
            String end_time,
            String order_status,
            String pay_time,
            String debtor,
            String principal,
            String interest,
            String create_time,
            String update_time,
            String record_flag,
            String remark,
            String laundry_uuid,
            String from_laundry_uuid,
            String debt_uuid,
            String debt_link_uuid,
            String from_debt_uuid,
            String root_code,
            String proto_version,
            String guarantee,
            String lend_out_type,
            String product_create_time
            )
    {
        this.id = Long.parseLong(id);
        this.uuid = new Text(uuid);
        this.link_uuid = new Text(link_uuid);
        this.link_length=Integer.parseInt(link_length);
        this.sequence=Integer.parseInt(sequence);
        this.from_user=new Text(from_user);
        this.to_user=new Text(to_user);
        this.product_uuid=new Text(product_uuid);
        this.product_code=new Text(product_code);
        this.product_type=Integer.parseInt(product_type);
        this.price=HiveDecimal.create(price);
        this.rate=HiveDecimal.create(rate);
        this.start_time= TimeParseUtil.parse(start_time);
        this.end_time= TimeParseUtil.parse(end_time);
        this.order_status=Integer.parseInt(order_status);
        this.pay_time= TimeParseUtil.parse(pay_time);
        this.debtor=new Text(debtor);
        this.principal=HiveDecimal.create(principal);
        this.interest=HiveDecimal.create(interest);
        this.create_time= TimeParseUtil.parse(create_time);
        this.update_time= TimeParseUtil.parse(update_time);
        this.record_flag=Integer.parseInt(record_flag);
        this.remark=new Text(remark);
        this.laundry_uuid=new Text(laundry_uuid);
        this.from_laundry_uuid=new Text(from_laundry_uuid);
        this.debt_uuid=new Text(debt_uuid);
        this.debt_link_uuid=new Text(debt_link_uuid);
        this.from_debt_uuid=new Text(from_debt_uuid);
        this.root_code=Long.parseLong(root_code);
        this.proto_version=new Text(proto_version);
        this.guarantee=new Text(guarantee);
        this.lend_out_type=Integer.parseInt(lend_out_type);
        this.product_create_time= TimeParseUtil.parse(product_create_time);
    }


    @Override
  public String toString() {
    return "MutableRecord [id=" + id + ", " + "rowId=" + rowId + "]";
  }

}
