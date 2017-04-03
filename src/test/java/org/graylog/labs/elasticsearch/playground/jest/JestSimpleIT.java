package org.graylog.labs.elasticsearch.playground.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_PASSWORD;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_URI;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_USER;
import static org.assertj.core.api.Assertions.assertThat;

public class JestSimpleIT {
    private JestClient client;

    @Before
    public void setUp() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(ES_URI)
                .defaultCredentials(ES_USER, ES_PASSWORD)
                .multiThreaded(true)
                .build());
        client = factory.getObject();
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.shutdownClient();
        }
    }

    @Test
    public void clusterHealth() throws IOException {
        final JestResult result = client.execute(new Health.Builder().build());
        assertThat(result.isSucceeded()).isTrue();
        assertThat(result.getResponseCode()).isEqualTo(200);
        assertThat(result.getJsonString()).isNotBlank();
    }

    @Test
    public void createAndDeleteIndex() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final JestResult result = client.execute(new CreateIndex.Builder(indexName).build());
        assertThat(result.isSucceeded()).isTrue();
        assertThat(result.getJsonString()).isNotBlank();

        final JestResult result2 = client.execute(new IndicesExists.Builder(indexName).build());
        assertThat(result2.isSucceeded()).isTrue();

        final JestResult result3 = client.execute(new DeleteIndex.Builder(indexName).build());
        assertThat(result3.isSucceeded()).isTrue();
    }

    @Test
    public void createAndDeleteDocument() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final JestResult createIndexResult = client.execute(new CreateIndex.Builder(indexName).build());
        assertThat(createIndexResult.isSucceeded()).isTrue();
        assertThat(createIndexResult.getJsonString()).isNotBlank();

        final JestResult indexExistsResult = client.execute(new IndicesExists.Builder(indexName).build());
        assertThat(indexExistsResult.isSucceeded()).isTrue();

        final Index index = new Index.Builder("{\"name\":\"Foobar\", \"number\": 42}")
                .index(indexName)
                .type("custom")
                .id("1")
                .build();
        final DocumentResult indexResult = client.execute(index);
        assertThat(indexResult.isSucceeded()).isTrue();

        final DocumentResult getResult = client.execute(new Get.Builder(indexName, "1").build());
        assertThat(getResult.isSucceeded()).isTrue();
        assertThat(getResult.getId()).isEqualTo("1");
        assertThat(getResult.getIndex()).isEqualTo(indexName);
        assertThat(getResult.getType()).isEqualTo("custom");
        assertThat(getResult.getSourceAsObject(Map.class).get("name")).isEqualTo("Foobar");
        assertThat(getResult.getSourceAsObject(Map.class).get("number")).isEqualTo(42.0);

        final String query = "{\n" +
                "    \"query\": {\n" +
                "        \"query_string\" : {\n" +
                "            \"query\" : \"name:Foo*\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        final SearchResult searchResult = client.execute(new Search.Builder(query).addIndex(indexName).build());
        assertThat(searchResult.isSucceeded()).isTrue();
        assertThat(searchResult.getTotal()).isEqualTo(1);

        final DocumentResult deleteResult = client.execute(new Delete.Builder("1").type("custom").index(indexName).build());
        assertThat(deleteResult.isSucceeded()).isTrue();
        assertThat(deleteResult.getId()).isEqualTo("1");
        assertThat(deleteResult.getIndex()).isEqualTo(indexName);
        assertThat(deleteResult.getType()).isEqualTo("custom");

        final JestResult deleteIndexResult = client.execute(new DeleteIndex.Builder(indexName).build());
        assertThat(deleteIndexResult.isSucceeded()).isTrue();
    }
}
