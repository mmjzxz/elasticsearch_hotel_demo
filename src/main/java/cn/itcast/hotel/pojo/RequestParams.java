package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * @author LEGION
 */
@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
}
