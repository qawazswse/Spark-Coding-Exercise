package receipts.analyze;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.Util.Util;
import receipts.case_objects.Receipt;
import receipts.case_objects.ReceiptItem;
import receipts.conf.DataSourceConf;
import receipts.result_objects.CategoryDiscountRecord;
import scala.Tuple2;

import java.io.IOException;

import static receipts.conf.DataSourceConf.OUTPUT_DIR;
import static receipts.csv_read.ReceiptItemRDD.setupReceiptsItemDataRdd;
import static receipts.csv_read.ReceiptRDD.setupReceiptsDataRdd;

public class ReceiptItemRddAnalyze {


    public static void main(String[] args) {

        int months = Integer.parseInt(args[0]);
        String fileType = args[1];

        // initialize
        System.setProperty("hadoop.home.dir", "c:/winutils");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // variables
        String fileName = "discount_percentage_each_category_in_last_" + months + "_months";
        JavaRDD<Receipt> receiptData = setupReceiptsDataRdd(sc, DataSourceConf.REWARDS_RECEIPTS_LAT_V3);;
        JavaRDD<ReceiptItem> receiptItemData = setupReceiptsItemDataRdd(sc, DataSourceConf.REWARDS_RECEIPTS_ITEM_LAT_V2);

        // join two JavaRDD together
        JavaPairRDD<String, Receipt> receiptJavaPairRdd = receiptData
                .mapToPair(row -> new Tuple2<>(row.getReceiptId(), row));

        JavaPairRDD<String, ReceiptItem> receiptItemJavaPairRdd = receiptItemData
                .mapToPair(row -> new Tuple2<>(row.getRewardsReceiptId(), row));

        JavaPairRDD<String, Tuple2<ReceiptItem, Receipt>> joinedRdd = receiptItemJavaPairRdd
                .join(receiptJavaPairRdd);

        // only choose the information in 'months' months
        JavaPairRDD<String, Tuple2<ReceiptItem, Receipt>> InMonthsRdd = joinedRdd
                .filter(
                        r -> r._2._2
                                .getReceiptPurchaseDate()
                                .after(Util.getDateByMonthBefore(months))
                );

        // put the filtered ReceiptItem objects into a JavaRDD
        JavaRDD<ReceiptItem> e = InMonthsRdd.map(r -> r._2._1);

        // get Category, DiscountedPrice, and ItemPrice from the raw data
        JavaPairRDD<String, Tuple2<Double, Double>> categoryPriceRdd = e
                .mapToPair(row -> new Tuple2<>(
                                row.getCategory(),
                                new Tuple2<>(
                                        row.getDiscountedPrice(),
                                        row.getItemPrice())
                        )
                );

        // remove rows that doesn't have enough information
        JavaPairRDD<String, Tuple2<Double, Double>> receiptsWithFullInformationRdd = categoryPriceRdd
                .filter(row -> row._1!=null && row._2._1!=null && row._2._2!=null && row._2._2!=0.0);


        // calculate total discount price and total price for each category
        JavaPairRDD<String, Tuple2<Double, Double>> categoryTotalPriceRdd = receiptsWithFullInformationRdd
                .reduceByKey((value1, value2) -> new Tuple2<>(
                                value1._1() + value2._1(),
                                value1._2() + value2._2()
                        )
                );

        // calculate discount percentage for each category
        JavaPairRDD<String, Double> categoryDiscountPercentage = categoryTotalPriceRdd
                .mapToPair(row -> new Tuple2<>(
                                row._1(),
                                1 - row._2._1/row._2._2
                        )
                );

        // sort by value(discount percentage)
        JavaPairRDD<String, Double> sortedByValueRdd = categoryDiscountPercentage
                .mapToPair(row -> new Tuple2<>(row._2, row._1))
                .sortByKey(false)
                .mapToPair(row -> new Tuple2<>(row._2, row._1));

        // print out the top 50 categories with the largest discount percentage
//        sortedByValueRdd.take(50).forEach(System.out::println);

        JavaRDD<CategoryDiscountRecord> rdd = sortedByValueRdd.map(row -> new CategoryDiscountRecord(row._1, row._2));

        JavaRDD<String> toFile = rdd.map(row -> row.getString(fileType));

        toFile.saveAsTextFile(OUTPUT_DIR + fileName);

        try {
            Util.toOneFile(toFile, fileName, fileType);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        System.out.println(
                fileName + "." + fileType + "\n"
                        + "is now ready in the output folder."
        );
    }
}
