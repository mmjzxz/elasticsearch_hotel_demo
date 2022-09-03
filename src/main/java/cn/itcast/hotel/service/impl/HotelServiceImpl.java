package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LEGION
 */
@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams requestParams) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            buildBasicQuery(requestParams, request);
            //分页
            int page = requestParams.getPage();
            int size = requestParams.getSize();
            request.source().from((page - 1) * size).size(size);
            //发送请求
            System.out.println(request);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildBasicQuery(RequestParams requestParams, SearchRequest request) {
        //构建BoolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        String key = requestParams.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //城市条件
        if (requestParams.getCity() != null && !requestParams.getCity().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", requestParams.getCity()));
        }
        //品牌
        if (requestParams.getBrand() != null && !requestParams.getBrand().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", requestParams.getBrand()));
        }
        //星级
        if (requestParams.getStarName() != null && !requestParams.getStarName().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", requestParams.getStarName()));
        }
        //价格
        if (requestParams.getMinPrice() != null && requestParams.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price")
                    .gte(requestParams.getMinPrice())
                    .lte(requestParams.getMaxPrice())
            );
        }
        request.source().query(boolQuery);
    }

    private PageResult handleResponse(SearchResponse response) {
        System.out.println(response);
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }
}
