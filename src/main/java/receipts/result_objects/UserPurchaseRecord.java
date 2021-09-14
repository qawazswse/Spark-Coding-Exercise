package receipts.result_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserPurchaseRecord implements ResultObject {

    private String userId;
    private Double purchase;

    @Override
    public String getJsonString() {
        return "{" +
                "userId='" + userId + '\'' +
                ", purchase=" + purchase +
                '}' + ',';
    }

    @Override
    public String getParquetString() {
        return "{" +
                "'userId':'" + userId + '\'' +
                ", 'purchase':" + purchase +
                '}';
    }

    @Override
    public String getCsvString() {
        return userId + "," + purchase;
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
