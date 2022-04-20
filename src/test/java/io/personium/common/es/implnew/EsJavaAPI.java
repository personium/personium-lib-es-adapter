package io.personium.common.es.implnew;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.junit.Test;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.personium.common.es.EsIndex;

public class EsJavaAPI {

    HttpHost targetEsHost;
    String targetIndex;

    public EsJavaAPI() {
        this.targetEsHost = new HttpHost("localhost", 9200);
        targetIndex = "index_for_test";
    }

    // private void createIndex(ElasticsearchClient client) throws IOException {
    //     var esIndex = new PersoniumEsIndexImpl();
    //     esIndex.createIndex(client, targetIndex);
    //     // client.indices().putMapping(s -> s.)
    // }

    @Test
    public void hoge() throws Exception {

        try (var restClient = RestClient.builder(targetEsHost)
                // .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                // @Override
                // public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                // return httpClientBuilder.setProxy(null);
                // }
                // })
                .build();) {
            var transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

            var client = new ElasticsearchClient(transport);
            var aClient = new ElasticsearchAsyncClient(transport);

            var esIndex = new PersoniumEsIndexImpl(transport);
            esIndex.putMapping();


            // cf.exceptionally(ex -> )
            // try {
            //     System.out.println(cf.get().toString());
            // } catch(ExecutionException e) {
            //     e.getCause()
            // }
            // // ElasticsearchAsyncClient esAsyncClient = new ElasticsearchAsyncClient(transport);

            // SearchResponse<JsonData> search = client.search(s -> s.index(targetIndex), JsonData.class);
        }
    }
}
