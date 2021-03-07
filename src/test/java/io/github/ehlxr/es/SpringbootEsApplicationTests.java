package io.github.ehlxr.es;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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

@SpringBootTest
class SpringbootEsApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void searchData() throws IOException {
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
