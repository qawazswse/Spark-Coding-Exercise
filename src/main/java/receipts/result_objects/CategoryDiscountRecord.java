package receipts.result_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class CategoryDiscountRecord implements Serializable {

    String category;
    Double discount;

}