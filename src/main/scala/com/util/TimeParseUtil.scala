package com.util

import java.sql.Timestamp

/**
  * Created by xiaoft on 2017/3/3.
  */
object TimeParseUtil {

  def parse(time: String): Timestamp = {
    if (!time.isEmpty) {
      Timestamp.valueOf(time)
    }
    else {
      Timestamp.valueOf("2017-02-24 00:21:55")
    }
  }
}
