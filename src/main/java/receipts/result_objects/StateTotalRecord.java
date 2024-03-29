package receipts.result_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class StateTotalRecord implements ResultObject {

    private String state;
    private Double total;

    @Override
    public String getJsonString() {
        return "{" +
                "userId='" + state + '\'' +
                ", purchase=" + total +
                '}' + ',';
    }

    @Override
    public String getParquetString() {
        return "{" +
                "'userId':'" + state + '\'' +
                ", 'purchase':" + total +
                '}';
    }

    @Override
    public String getCsvString() {
        return state + "," + total;
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
