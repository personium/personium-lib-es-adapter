package io.personium.common.es.implnew;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.analysis.Language;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import io.personium.common.es.EsIndex;

public class PersoniumEsIndexImpl {

    ElasticsearchAsyncClient aClient;
    ElasticsearchClient client;

    public PersoniumEsIndexImpl(RestClientTransport transport) {
        aClient = new ElasticsearchAsyncClient(transport);
        client = new ElasticsearchClient(transport);
    }

    Integer getMaxThreadCount() {
        var configVal = System.getProperty("io.personium.es.index.merge.scheduler.maxThreadCount");
        if (configVal != null) {
            return Integer.parseInt(configVal);
        }
        return null;
    }

    public void createIndex(ElasticsearchClient client, String indexName) throws IOException {
        // try (InputStream is =
        // PersoniumEsIndexImpl.class.getClassLoader().getResourceAsStream("esnext/indexSettings.json")) {
        // client.indices().create(cir -> cir.index(indexName)
        // .settings(s -> s.withJson(is).mergeSchedulerMaxThreadCount(getMaxThreadCount())));
        // }
        client.indices().create(cir -> cir.index(indexName));

        // put each mapping;
        ElasticsearchAsyncClient aClient;
        // aClient.
    }

    public CompletableFuture<DeleteIndexResponse> deleteIndex(ElasticsearchAsyncClient aClient,
            String category,
            String type) {
        return deleteIndex(aClient, composePersoniumIndexString(category, type));
    }

    public CompletableFuture<DeleteIndexResponse> deleteIndex(ElasticsearchAsyncClient aClient, String indexName) {
        return aClient.indices().delete(dir -> dir.index(indexName));
    }

    public void putMapping() throws IOException {

        Map<String, String> mappings = new HashMap<String, String>();

        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "link"), "esnext/mapping/link.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "Account"), "esnext/mapping/account.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "Box"), "esnext/mapping/box.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "Role"), "esnext/mapping/role.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "Relation"), "esnext/mapping/relation.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "SentMessage"),
        "esnext/mapping/sentMessage.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "ReceivedMessage"),
        "esnext/mapping/receivedMessage.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "EntityType"),
        "esnext/mapping/entityType.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "AssociationEnd"),
        "esnext/mapping/associationEnd.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "Property"), "esnext/mapping/property.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "ComplexType"),
        "esnext/mapping/complexType.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "ComplexTypeProperty"),
        "esnext/mapping/complexTypeProperty.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "ExtCell"), "esnext/mapping/extCell.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "ExtRole"), "esnext/mapping/extRole.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "dav"), "esnext/mapping/dav.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "UserData"), "esnext/mapping/userdata.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_USR, "Rule"), "esnext/mapping/rule.json");

        // initialize index `ad`
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_AD, "Domain"), "esnext/mapping/domain.json");
        mappings.put(composePersoniumIndexString(EsIndex.CATEGORY_AD, "Cell"), "esnext/mapping/cell.json");

        List<CompletableFuture<Void>> listCf = new ArrayList<CompletableFuture<Void>>();

        for (String index : mappings.keySet()) {
            var cf = aClient.indices().exists(er -> er.index(Arrays.asList(index)))
                .thenApply(isExist -> {
                    if (isExist.value()) {
                        return aClient.indices().delete(dir -> dir.index(Arrays.asList(index)))
                            .thenApply(a -> CompletableFuture.completedFuture(isExist.value()));
                    }
                    return CompletableFuture.completedFuture(isExist.value());
                })
                .thenRunAsync(() -> {
                    //TODO ここもっときれいに
                    System.out.println(index);
                    try (InputStream is = PersoniumEsIndexImpl.class.getClassLoader()
                        .getResourceAsStream(mappings.get(index))) {
                        aClient.indices().create(cir -> cir.index(index).mappings(ms -> ms.withJson(is))).join();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

            listCf.add(cf);
            System.out.println("end: " + index);
        }

        listCf.forEach(CompletableFuture::join);
        // // load mappings
        // try (InputStream mis = {
        // return aClient.indices().create(cir -> cir.index(composePersoniumIndexString(EsIndex.CATEGORY_AD, "Domain"))
        // // .settings(is -> is.withJson(mis))
        // .mappings(ms -> ms.withJson(mis)));
        // }
    }

    void loadMappingConfigs() {
        // mappingConfigs = new HashMap<String, Map<String, JSONObject>>();

        // PutMappingRequest pmr = new PutMappingRequest.Builder().withJson(input);

    }

    public void createIndex(String index, String type) throws IOException {
        String indexString = composePersoniumIndexString(index, type);
        BooleanResponse br = client.indices().exists(er -> er.index(indexString));
        if (br.value()) {
            client.indices().delete(dir -> dir.index(indexString));
        }
        client.indices().create(cir -> cir.index(indexString));
    }

    public static String composePersoniumIndexString(String index, String type) {
        if (type == null) {
            return index + "." + "*";
        }
        return index + "." + type.toLowerCase();
    }

}
