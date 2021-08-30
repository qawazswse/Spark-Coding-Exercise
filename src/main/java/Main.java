import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import receipts.case_objects.Receipt;
import receipts.case_objects.ReceiptItem;
import receipts.conf.DataSource;

import static receipts.Util.Util.*;
import static receipts.csv_read.ReceiptItemRDD.setupReceiptsItemDataRdd;
import static receipts.csv_read.ReceiptRDD.setupReceiptsDataRdd;
import static receipts.service.ReceiptRddService.stateTotalCount;
import static receipts.service.ReceiptItemRddService.categoryDiscount;
import static receipts.service.ReceiptSQLService.userTotalPurchase;

public class Main {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "c:/winutils");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // analyze 1:   calculate total receipt price for each state in descending order in last 24 months
        // output in JSON format
        JavaRDD<Receipt> ReceiptData = setupReceiptsDataRdd(sc, DataSource.REWARDS_RECEIPTS_LAT_V3);
        JavaPairRDD<String, Double> result1 = stateTotalCount(ReceiptData, 24);
        rddToJSON(result1, "state_total_count_result");

        // analyze 2:   calculate average discount percentage for each category in descending order in last 20 months
        // output in csv format
        JavaRDD<ReceiptItem> ReceiptItemData = setupReceiptsItemDataRdd(sc, DataSource.REWARDS_RECEIPTS_ITEM_LAT_V2);
        JavaPairRDD<String, Double> result2 = categoryDiscount(ReceiptData, ReceiptItemData, 20);
        rddToCSV(result2, "category_discount_result");

        // analyze 3:   calculate total receipt price for each user in descending order in last 16 months
        // output in parquet format
        Dataset<Row> result3 = userTotalPurchase(16);
        datasetToParquet(result3, "user_total_purchase_result");
    }

}
