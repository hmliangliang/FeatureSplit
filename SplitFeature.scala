package com.dm.activity

import com.utils.TDWClient
import org.apache.spark.mllib.util.OptionParser

object SplitFeature {
  def main(args: Array[String]): Unit = {

    // 初始化spark环境
    val options = new OptionParser(args)
    val client = new TDWClient()

    // 输入的信息表格
    val Array(input_db,input_tb) =
      options.getString("data_input").split(",")(0).split("::")
    val resultRdd = client
      .tdw(input_db)
      .table(input_tb)
      .map {
          value=> {
            var array1 = Array(value(0))
            for(i<- 1 until value.length-2){
              array1 = array1 ++ Array(value(i))
            }
            val array2 = value(value.length-2).split(" ")
            val array3 = value(value.length-1).split(" ")
            array1 ++ array2 ++ array3
          }
      }
    val Array(resultdb, resulttable) =
      options.getString("data_output").split(",")(0).split("::")
    client.writeRDDToTable(
      resultdb,
      resulttable,
      resultRdd,
      partValue = "",
      replace = false
    )

  }
}
