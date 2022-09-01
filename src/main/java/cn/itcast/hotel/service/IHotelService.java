package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * @author LEGION
 */
public interface IHotelService extends IService<Hotel> {
    /**
     * 查询酒店信息
     * @param requestParams
     * @return
     */
    PageResult search(RequestParams requestParams);
}
