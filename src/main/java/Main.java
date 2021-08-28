import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple11;
import scala.Tuple15;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "c:/hadoop");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        




    }

    private static JavaPairRDD<String, Tuple15> setupReceiptsDataRdd(JavaSparkContext sc, boolean testMode) {
        if(testMode) {

        }

        return sc.textFile("src/main/resources/receipts/rewards_receipts_lat_v3.csv")
                .mapToPair(row -> {
                    String[] cols = row.split(",");
                    return new Tuple2<>(cols[0], new Tuple15<>(
                                    cols[1],
                                    cols[2],
                                    cols[3],
                                    cols[4],
                                    Integer.parseInt(cols[5]),
                                    cols[6],
                                    Integer.parseInt(cols[7]),
                                    cols[8],
                                    cols[9],
                                    cols[10],
                                    Double.parseDouble(cols[11]),
                                    Integer.parseInt(cols[12]),
                                    cols[13],
                                    cols[14],
                                    Boolean.parseBoolean(cols[15])
                    ));
                });
    }

    private static JavaPairRDD<String, Tuple11> setupReceiptItemsDataRdd(JavaSparkContext sc, boolean testMode) {
        if(testMode) {

        }

        return sc.textFile("src/main/resources/receipts/rewards_receipts_item_lat_v3.csv")
                .mapToPair(row -> {
                    String[] cols = row.split(",");
                    return new Tuple2<>(cols[0], new Tuple11<>(
                            Integer.parseInt(cols[1]),
                            cols[2],
                            cols[3],
                            cols[4],
                            Integer.parseInt(cols[5]),
                            Double.parseDouble(cols[6]),
                            Double.parseDouble(cols[7]),
                            Double.parseDouble(cols[8]),
                            cols[9],
                            cols[10],
                            cols[11]
                    ));
                });
    }

}
