package receipts.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import receipts.Util.Util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static receipts.conf.DataSource.*;

public class ReceiptSQLService {


    /*
        calculate total receipt price for each user in descending order in last 16 months
        input: how many month before
        output: Dataset of result
     */

    public static Dataset<Row> userTotalPurchase(Integer m) {

        // initialize
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.wareHouse.dir", "file:///c:tmp/")
                .getOrCreate();

        // read CSV file to Dataset
        Dataset<Row> receiptDataset = spark.read().option("header", true).csv(REWARDS_RECEIPTS_LAT_V3);

        // make a temp view for the Dataset
        receiptDataset.createOrReplaceTempView("RECEIPTS");

        // set the current Date string to compare with the Date records in the Dataset
        Date mMonthBefore = Util.getDateByMonthBefore(m);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.ssss");
        String date = df.format(mMonthBefore);

        // use sql text to get the result Dataset
        Dataset<Row> userPurchaseCount = spark.sql(
                "select USER_ID, SUM (RECEIPT_TOTAL) as SUM_RECEIPT_TOTAL " +
                        "from RECEIPTS " +
                        "where RECEIPT_PURCHASE_DATE > '" + date + "' " +
                        "group by USER_ID " +
                        "order by SUM_RECEIPT_TOTAL DESC");

        // print first 70 records
        userPurchaseCount.show(70);

        return userPurchaseCount;
    }


}
