package org.graylog.labs.elasticsearch.playground;

public class ElasticsearchDocker {
    public static final String ES_HOST = System.getProperty("es.host", "127.0.0.1");
    public static final int ES_PORT = Integer.getInteger("es.port", 9200);
    public static final String ES_URI = "http://" + ES_HOST + ":" + ES_PORT;
    public static final String ES_USER = "elastic";
    public static final String ES_PASSWORD = "changeme";
}
