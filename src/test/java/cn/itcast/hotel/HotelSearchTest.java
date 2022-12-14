package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

public class HotelSearchTest {
    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.139.130:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

//    private void handleResponse(SearchResponse response) {
//        SearchHits searchHits = response.getHits();
//        long total = searchHits.getTotalHits().value;
//        System.out.println("????????????" + total + "?????????");
//        SearchHit[] hits = searchHits.getHits();
//        for (SearchHit hit : hits) {
//            String json = hit.getSourceAsString();
//            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
//            System.out.println("hotelDoc = " + hotelDoc);
//        }
//    }

    private void handleResponse(SearchResponse response) {
        // 4.????????????
        SearchHits searchHits = response.getHits();
        // 4.1.???????????????
        long total = searchHits.getTotalHits().value;
        System.out.println("????????????" + total + "?????????");
        // 4.2.????????????
        SearchHit[] hits = searchHits.getHits();
        // 4.3.??????
        for (SearchHit hit : hits) {
            // ????????????source
            String json = hit.getSourceAsString();
            // ????????????
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // ??????????????????
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // ?????????????????????????????????
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    // ???????????????
                    String name = highlightField.getFragments()[0].string();
                    // ?????????????????????
                    hotelDoc.setName(name);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }

    /**
     * match??????
     */

    @Test
    void testMatchAll() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @Test
    void testMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "??????"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @Test
    void testMultiMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.multiMatchQuery("??????", "name", "business"));
        request.source().query(QueryBuilders.multiMatchQuery("??????", "name", "business"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    /**
     * ????????????
     * term?????????????????????
     * range???????????????
     */
    @Test
    void testTerm() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.termQuery("city", "??????"));
        request.source().query(QueryBuilders.termQuery("city", "??????"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @Test
    void testRange() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.rangeQuery("price").gte(100).lte(150));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    /**
     * ????????????
     */
    @Test
    void testBool() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("city", "??????"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(boolQuery);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    /**
     * ????????????
     */
    @Test
    void testPageAndSort() throws IOException {
        // ?????????????????????
        int page = 1, size = 5;

        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2.?????? sort
        request.source().sort("price", SortOrder.ASC);
        // 2.3.?????? from???size
        request.source().from((page - 1) * size).size(5);
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.????????????
        handleResponse(response);

    }

    /**
     * ??????
     */
    @Test
    void testHighlight() throws IOException {
        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchQuery("all", "??????"));
        // 2.2.??????
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.????????????
        handleResponse(response);
    }

    @Test
    void testAggregation() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().size(0);
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(20));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponseAggregation(response);
    }

    private void handleResponseAggregation(SearchResponse response) {
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }
    }
}
