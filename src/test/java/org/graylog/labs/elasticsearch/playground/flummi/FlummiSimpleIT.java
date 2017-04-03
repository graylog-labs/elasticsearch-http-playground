package org.graylog.labs.elasticsearch.playground.flummi;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Realm;
import de.otto.flummi.ClusterHealthResponse;
import de.otto.flummi.ClusterHealthStatus;
import de.otto.flummi.Flummi;
import de.otto.flummi.query.QueryBuilders;
import de.otto.flummi.request.GsonHelper;
import de.otto.flummi.response.GetResponse;
import de.otto.flummi.response.SearchHit;
import de.otto.flummi.response.SearchHits;
import de.otto.flummi.response.SearchResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.UUID;

import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_PASSWORD;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_URI;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_USER;
import static org.assertj.core.api.Assertions.assertThat;

public class FlummiSimpleIT {
    private Flummi flummi;

    @Before
    public void setup() {
        final Realm realm = new Realm.RealmBuilder().setPrincipal(ES_USER)
                .setPassword(ES_PASSWORD)
                .build();
        final AsyncHttpClientConfig clientConfig = new AsyncHttpClientConfig.Builder()
                .setRealm(realm)
                .build();
        final AsyncHttpClient client = new AsyncHttpClient(clientConfig);
        flummi = new Flummi(client, ES_URI);
    }

    @Test
    public void clusterHealth() {
        final ClusterHealthResponse response = flummi.admin().cluster().prepareHealth().execute();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isNotEqualTo(ClusterHealthStatus.RED);
    }

    @Test
    public void createAndDeleteIndex() {
        final String indexName = UUID.randomUUID().toString();
        flummi.admin().indices().prepareCreate(indexName).execute();

        final Boolean indexExists = flummi.admin().indices().prepareExists(indexName).execute();
        assertThat(indexExists).isTrue();

        flummi.admin().indices().prepareDelete(indexName).execute();
    }

    @Test
    public void createAndDeleteDocument() {
        final String indexName = UUID.randomUUID().toString();
        flummi.admin().indices().prepareCreate(indexName).execute();

        final Boolean indexExists = flummi.admin().indices().prepareExists(indexName).execute();
        assertThat(indexExists).isTrue();

        final JsonObject documentSource = GsonHelper.object(
                "name", new JsonPrimitive("Foobar"),
                "number", new JsonPrimitive(42L)
        );
        flummi.prepareIndex()
                .setIndexName(indexName)
                .setId(1)
                .setDocumentType("custom")
                .setSource(documentSource)
                .execute();

        final GetResponse getResponse = flummi.prepareGet(indexName, "custom", "1").execute();
        assertThat(getResponse.isExists()).isTrue();
        assertThat(getResponse.getId()).isEqualTo("1");
        assertThat(getResponse.getSource()).isEqualTo(documentSource);

        final JsonObject termQuery = QueryBuilders.wildcardQuery("name", "*").build();
        final SearchResponse searchResponse = flummi.prepareSearch(indexName)
                .setSize(10)
                .setQuery(termQuery)
                .execute();
        final SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getTotalHits()).isEqualTo(1L);

        final Iterator<SearchHit> searchHitIterator = searchHits.iterator();
        assertThat(searchHitIterator.hasNext()).isTrue();
        final SearchHit searchHit = searchHitIterator.next();
        assertThat(searchHit.getId()).isEqualTo("1");
        assertThat(searchHit.getSource()).isEqualTo(documentSource);

        flummi.prepareDelete().setIndexName(indexName)
                .setId("1")
                .setDocumentType("custom")
                .execute();

        flummi.admin().indices().prepareDelete(indexName).execute();
    }
}
