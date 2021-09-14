package receipts.analyze;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import receipts.Util.Util;
import receipts.result_objects.UserPurchaseRecord;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static receipts.conf.DataSourceConf.*;
import static org.apache.spark.sql.functions.*;

public class ReceiptSQLAnalyze {

    public static void main(String[] args) {

        int months = Integer.parseInt(args[0]);
        String fileType = args[1];

        String fileName = "user_total_purchase_in_last_" + months + "_months";

        // initialize
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.wareHouse.dir", "file:///c:tmp/")
                .getOrCreate();

        // read CSV file to Dataset
        Dataset<Row> receiptDataset = spark.read().option("header", true).csv(REWARDS_RECEIPTS_LAT_V3);

        // make a temp view for the Dataset
        receiptDataset.createOrReplaceTempView("RECEIPTS");

        // set the current Date string to compare with the Date records in the Dataset
        Date mMonthBefore = Util.getDateByMonthBefore(months);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.ssss");
        String date = df.format(mMonthBefore);

        // use spark sql api to get the result Dataset
        Dataset<Row> userPurchaseCount = receiptDataset
                .where(col("RECEIPT_PURCHASE_DATE").$greater(date))
                .groupBy("USER_ID")
                .agg(sum("RECEIPT_TOTAL").as("SUM_RECEIPT_TOTAL"))
                .select("USER_ID", "SUM_RECEIPT_TOTAL")
                .orderBy(col("SUM_RECEIPT_TOTAL").desc());

        // use sql text to get the result Dataset
//        Dataset<Row> userPurchaseCount = spark.sql(
//                "select USER_ID, SUM (RECEIPT_TOTAL) as SUM_RECEIPT_TOTAL " +
//                        "from RECEIPTS " +
//                        "where RECEIPT_PURCHASE_DATE > '" + date + "' " +
//                        "group by USER_ID " +
//                        "order by SUM_RECEIPT_TOTAL DESC");

        // print first 70 records
//        userPurchaseCount.show(70);

        JavaRDD<UserPurchaseRecord> rdd = userPurchaseCount
                .toJavaRDD()
                .map(row -> new UserPurchaseRecord((String)row.get(0), (Double)row.get(1)));

        JavaRDD<String> toFile = rdd.map(row -> row.getString(fileType));

        toFile.saveAsTextFile(OUTPUT_DIR + fileName);

        try {
            Util.toOneFile(toFile, fileName, fileType);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(
                fileName + "." + fileType + "\n"
                        + "is now ready in the output folder."
        );
    }


}
