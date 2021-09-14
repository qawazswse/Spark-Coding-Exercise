package receipts.result_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CategoryDiscountRecord implements ResultObject {

    private String category;
    private Double discount;

    @Override
    public String getJsonString() {
        return "{" +
                "userId='" + category + '\'' +
                ", purchase=" + discount +
                '}' + ',';
    }

    @Override
    public String getParquetString() {
        return "{" +
                "'userId':'" + category + '\'' +
                ", 'purchase':" + discount +
                '}';
    }

    @Override
    public String getCsvString() {
        return category + "," + discount;
    }

    @Override
    public String getString(String fileType) {
        switch (fileType) {
            case "json":
                return getJsonString();
            case "parquet":
                return getParquetString();
            case "csv":
                return getCsvString();
        }
        return null;
    }

}
