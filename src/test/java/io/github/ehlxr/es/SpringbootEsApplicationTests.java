package io.github.ehlxr.es;

import io.github.ehlxr.es.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.stream.IntStream;

@SpringBootTest
class SpringbootEsApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    static class User {
        private String name;
        private Integer gender;
        private Integer age;
        private String email;
    }

    @Test
    void indexTest() throws IOException {
        IndexRequest request = new IndexRequest("my_idx");
        request.id("10");
        request.ifSeqNo();
        request.source(JsonUtils.obj2String(new User("zhansan", 1, 23, "zhansan@test.com")), XContentType.JSON);

        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    void deleteIndexTest() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("my_idx");
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest request = new DeleteIndexRequest("my_idx");
            request.timeout(TimeValue.timeValueMinutes(2));

            AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            System.out.println(response.isAcknowledged());
        }
    }

    @Test
    void bulkTest() throws IOException {
        BulkRequest request = new BulkRequest("my_idx");

        IntStream.range(0, 10).forEach(i -> {
            IndexRequest indexRequest = new IndexRequest("my_idx")
                    .id("" + i)
                    .source(JsonUtils.obj2String(new User("zhansan" + 1, i % 2, 20 + i, i + "zhansan@test.com")), XContentType.JSON);

            System.out.println(indexRequest.toString());
            request.add(indexRequest);
        });


        BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.hasFailures());
    }


    @Test
    void searchTest() throws IOException {
        /*
         * GET bank/_search
         * {
         *   "query": {
         *     "match_all": {}
         *   },
         *   "aggs": {
         *     "ageAgg": {
         *       "terms": {
         *         "field": "age",
         *         "size": 100
         *       },
         *       "aggs": {
         *         "genderAgg": {
         *           "terms": {
         *             "field": "gender.keyword",
         *             "size": 10
         *           },
         *           "aggs": {
         *             "balanceAvg": {
         *               "avg": {
         *                 "field": "balance"
         *               }
         *             }
         *           }
         *         },
         *         "balanceAvg":{
         *           "avg": {
         *             "field": "balance"
         *           }
         *         }
         *       }
         *     }
         *   },
         *   "size": 0
         *
         * }
         */
        TermsAggregationBuilder ageAgg = AggregationBuilders.terms("ageAgg");
        ageAgg.field("age").size(100);

        ageAgg.subAggregation(AggregationBuilders.avg("balanceAvg").field("balance"));

        TermsAggregationBuilder genderAgg = AggregationBuilders.terms("genderAgg").field("gender.keyword");
        genderAgg.subAggregation(AggregationBuilders.avg("ageBalanceAvg").field("balance"));

        ageAgg.subAggregation(genderAgg);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.aggregation(ageAgg);
        sourceBuilder.size(0);

        SearchRequest request = new SearchRequest("bank");
        request.source(sourceBuilder);

        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // System.out.println(response);

        Aggregations aggregations = response.getAggregations();

        Terms ageAggTerms = aggregations.get("ageAgg");
        ageAggTerms.getBuckets().forEach(ageAggBucket -> {
            Avg balanceAvg = ageAggBucket.getAggregations().get("balanceAvg");

            System.out.println("年龄为 " + ageAggBucket.getKeyAsString() + " 的人数 " + ageAggBucket.getDocCount() + " 平均工资为 " + balanceAvg.getValue());

            Terms genderAggTerms = ageAggBucket.getAggregations().get("genderAgg");
            genderAggTerms.getBuckets().forEach(genderAggBucket -> {
                Avg ageBalanceAvg = genderAggBucket.getAggregations().get("ageBalanceAvg");
                System.out.println("  其中性别为 " + genderAggBucket.getKeyAsString() + " 的人数为 " + genderAggBucket.getDocCount() + " 平均工资为 " + ageBalanceAvg.getValue());
            });

        });
    }

}
