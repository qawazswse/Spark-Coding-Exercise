package receipts.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import receipts.Util.Util;
import receipts.case_objects.Receipt;
import receipts.case_objects.ReceiptItem;
import receipts.result_objects.StateTotalRecord;
import scala.Tuple2;

public class ReceiptItemRddService {


    /*
        calculate average discount percentage for each category in descending order
        input: raw data JavaPairRDD<Receipt>, raw data JavaPairRDD<ReceiptItem>, and how many month before
        output: counted JavaPairRDD<String, Double> in descending order
     */

    public static Dataset<Row> categoryDiscount(
            JavaRDD<Receipt> receiptData, JavaRDD<ReceiptItem> itemData, Integer m) {

        // join two JavaRDD together
        JavaPairRDD<String, Receipt> receiptJavaPairRdd = receiptData
                .mapToPair(row -> new Tuple2<>(row.getReceiptId(), row));

        JavaPairRDD<String, ReceiptItem> receiptItemJavaPairRdd = itemData
                .mapToPair(row -> new Tuple2<>(row.getRewardsReceiptId(), row));

        JavaPairRDD<String, Tuple2<ReceiptItem, Receipt>> joinedRdd = receiptItemJavaPairRdd
                .join(receiptJavaPairRdd);

        // only choose the information in 'm' months
        JavaPairRDD<String, Tuple2<ReceiptItem, Receipt>> InMonthsRdd = joinedRdd
                .filter(
                        r -> r._2._2
                                .getReceiptPurchaseDate()
                                .after(Util.getDateByMonthBefore(m))
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
        sortedByValueRdd.take(50).forEach(System.out::println);

        JavaRDD<StateTotalRecord> rdd = sortedByValueRdd.map(row -> new StateTotalRecord(row._1, row._2));

        SparkSession spark = SparkSession.builder().appName("categoryDiscount").master("local[*]")
                .config("spark.sql.wareHouse.dir", "file:///c:tmp/")
                .getOrCreate();

        return spark.createDataFrame(rdd, StateTotalRecord.class);
    }
}
