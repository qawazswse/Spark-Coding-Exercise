package receipts.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import receipts.case_objects.ReceiptItem;
import scala.Tuple2;

public class ReceiptItemRddService {

    /*
        calculate average discount percentage for each category in descending order
        input: raw data JavaPairRDD<ReceiptItem>
        output: counted JavaPairRDD<String, Double> in descending order
     */

    public static JavaPairRDD<String, Double> categoryDiscount(JavaRDD<ReceiptItem> initData) {

        // get Category, DiscountedPrice, and ItemPrice from the raw data
        JavaPairRDD<String, Tuple2<Double, Double>> categoryPriceRdd = initData
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

        return sortedByValueRdd;
    }
}