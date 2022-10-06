/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.sql.internal.SQLConf

object Experiment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[8]")
            .config(SQLConf.CBO_ENABLED.key, "true")
            .config(SQLConf.JOIN_REORDER_ENABLED.key, "true")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .config("spark.ui.enabled", true) // http://192.168.0.80:4040/jobs/
      .config("spark.ui.port", 4040)
      .getOrCreate()

    val tables = Seq("call_center", "catalog_sales", "customer_demographics", "date_dim",
      "household_demographics", "item", "ship_mode", "store_sales", "catalog_page",
      "customer_address", "time_dim", "income_band", "promotion", "store", "web_returns",
      "catalog_returns", "customer", "inventory", "reason", "store_returns", "warehouse",
      "web_sales", "web_page", "web_site")

    // table registratrion
    spark.time {
      tables.foreach(t => {
        val path = s"/mnt/bigdata/tpcds/sf1-parquet/$t"
        //      val path = s"/mnt/bigdata/tpcds/sf100-parquet/$t.parquet"
        println(path)
        spark.read
          .parquet(path).createOrReplaceTempView(t)
      })
    }

    // intial planning
    val df = spark.time(spark.sql(q72))

    // execution
    spark.time(df.collect())

    // scalastyle:off println
    val plan = df.queryExecution.executedPlan
    println(plan)
    // scalastyle:on println

    // keep Spark web UI alive so I can view the plan
//    while (true) {
//      Thread.sleep(1000)
//    }
  }

  val q72 =
    """
  select  i_item_desc
       ,w_warehouse_name
       ,d1.d_week_seq
       ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
       ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
       ,count(*) total_cnt
  from catalog_sales
           join inventory on (cs_item_sk = inv_item_sk)
           join warehouse on (w_warehouse_sk=inv_warehouse_sk)
           join item on (i_item_sk = cs_item_sk)
           join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
           join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
           join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
           join date_dim d2 on (inv_date_sk = d2.d_date_sk)
           join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
           left outer join promotion on (cs_promo_sk=p_promo_sk)
           left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
  where d1.d_week_seq = d2.d_week_seq
    and inv_quantity_on_hand < cs_quantity
    and d3.d_date > d1.d_date + 5
    and hd_buy_potential = '1001-5000'
    and d1.d_year = 2000
    and cd_marital_status = 'W'
  group by i_item_desc,w_warehouse_name,d1.d_week_seq
  order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
      LIMIT 100"""

  val q72handTunedByMatt =
    """
      |select  i_item_desc
      |      ,w_warehouse_name
      |      ,d1.d_week_seq
      |      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      |      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      |      ,count(*) total_cnt
      |from catalog_sales
      |join item on (i_item_sk = cs_item_sk)
      |join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
      |join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
      |join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
      |join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
      |join inventory on (cs_item_sk = inv_item_sk)
      |join date_dim d2 on (inv_date_sk = d2.d_date_sk)
      |join warehouse on (w_warehouse_sk=inv_warehouse_sk)
      |left outer join promotion on (cs_promo_sk=p_promo_sk)
      |left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
      |where d1.d_week_seq = d2.d_week_seq
      |  and inv_quantity_on_hand < cs_quantity
      |  and d3.d_date > d1.d_date + 5
      |  and hd_buy_potential = '1001-5000'
      |  and d1.d_year = 2001
      |  and cd_marital_status = 'M'
      |group by i_item_desc,w_warehouse_name,d1.d_week_seq
      |order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
      | LIMIT 100;
      |""".stripMargin

}
