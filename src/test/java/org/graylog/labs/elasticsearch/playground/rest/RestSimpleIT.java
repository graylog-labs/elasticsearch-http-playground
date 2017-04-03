package org.graylog.labs.elasticsearch.playground.rest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_HOST;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_PASSWORD;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_PORT;
import static org.graylog.labs.elasticsearch.playground.ElasticsearchDocker.ES_USER;
import static org.assertj.core.api.Assertions.assertThat;

public class RestSimpleIT {
    private RestClient client;
    private Sniffer sniffer;

    @Before
    public void setUp() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(ES_USER, ES_PASSWORD));

        final HttpHost httpHost = new HttpHost(ES_HOST, ES_PORT);
        client = RestClient.builder(httpHost)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .build();

        // Requests fail if Sniffer is being used
        // sniffer = Sniffer.builder(client).build();
    }

    @After
    public void tearDown() throws IOException {
        if (null != sniffer) {
            sniffer.close();
        }
        if (null != client) {
            client.close();
        }
    }

    @Test
    public void clusterHealth() throws IOException {
        final Response response = client.performRequest("GET", "/_cluster/health");
        assertThat(response).isNotNull();
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    }

    @Test
    public void createAndDeleteIndex() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final Response response = client.performRequest("PUT", "/" + indexName);
        assertThat(response).isNotNull();
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

        final Response response2 = client.performRequest("HEAD", "/" + indexName);
        assertThat(response2).isNotNull();
        assertThat(response2.getStatusLine().getStatusCode()).isEqualTo(200);

        final Response response3 = client.performRequest("DELETE", "/" + indexName);
        assertThat(response3).isNotNull();
        assertThat(response3.getStatusLine().getStatusCode()).isEqualTo(200);
    }

    @Test
    public void createAndDeleteDocument() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final Response createIndexResponse = client.performRequest("PUT", "/" + indexName);
        assertThat(createIndexResponse).isNotNull();
        assertThat(createIndexResponse.getStatusLine().getStatusCode()).isEqualTo(200);

        final Response indexExistsResponse = client.performRequest("HEAD", "/" + indexName);
        assertThat(indexExistsResponse).isNotNull();
        assertThat(indexExistsResponse.getStatusLine().getStatusCode()).isEqualTo(200);

        final String document = "{\"name\":\"Foobar\", \"number\": 42}";
        final BasicHttpEntity httpEntity = new BasicHttpEntity();
        httpEntity.setContent(new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8)));
        httpEntity.setContentType("application/json");

        final Response indexResponse = client.performRequest("PUT", "/" + indexName + "/custom/1", Collections.emptyMap(), httpEntity);
        assertThat(indexResponse).isNotNull();
        assertThat(indexResponse.getStatusLine().getStatusCode()).isEqualTo(201);

        final Response getResponse = client.performRequest("GET", "/" + indexName + "/custom/1");
        assertThat(getResponse).isNotNull();
        assertThat(getResponse.getStatusLine().getStatusCode()).isEqualTo(200);
        // Poor man's result check
        final String getResponseEntity = EntityUtils.toString(getResponse.getEntity(), StandardCharsets.UTF_8);
        assertThat(getResponseEntity).contains("\"found\":true");
        assertThat(getResponseEntity).contains("\"_id\":\"1\"");
        assertThat(getResponseEntity).contains("\"_source\":" + document);

        final String query = "{\n" +
                "    \"query\": {\n" +
                "        \"query_string\" : {\n" +
                "            \"query\" : \"name:Foo*\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        final BasicHttpEntity queryEntity = new BasicHttpEntity();
        queryEntity.setContent(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)));
        queryEntity.setContentType("application/json");
        final Response queryResponse = client.performRequest("GET", "/" + indexName + "/custom/1", Collections.emptyMap(), queryEntity);
        assertThat(queryResponse).isNotNull();
        assertThat(queryResponse.getStatusLine().getStatusCode()).isEqualTo(200);

        // Poor man's result check
        final String queryResponseEntity = EntityUtils.toString(queryResponse.getEntity(), StandardCharsets.UTF_8);
        assertThat(queryResponseEntity).contains("\"found\":true");
        assertThat(queryResponseEntity).contains("\"_id\":\"1\"");
        assertThat(queryResponseEntity).contains("\"_source\":" + document);

        final Response deleteResponse = client.performRequest("DELETE", "/" + indexName + "/custom/1");
        assertThat(deleteResponse).isNotNull();
        assertThat(deleteResponse.getStatusLine().getStatusCode()).isEqualTo(200);

        final Response deleteIndexResponse = client.performRequest("DELETE", "/" + indexName);
        assertThat(deleteIndexResponse).isNotNull();
        assertThat(deleteIndexResponse.getStatusLine().getStatusCode()).isEqualTo(200);
    }
}
