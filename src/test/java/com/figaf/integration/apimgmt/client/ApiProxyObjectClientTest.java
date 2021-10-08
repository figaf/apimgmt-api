package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.data_provider.AgentTestDataProvider;
import com.figaf.integration.apimgmt.entity.ApiProxyMetaData;
import com.figaf.integration.common.data_provider.AgentTestData;
import com.figaf.integration.common.entity.RequestContext;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilya Nesterov
 */
@Slf4j
class ApiProxyObjectClientTest {

    private static final String API_TEST_API_PROXY_NAME = "FigafApiTestApiProxy";

    private static ApiProxyObjectClient apiProxyObjectClient;

    @BeforeAll
    static void setUp() {
        apiProxyObjectClient = new ApiProxyObjectClient(new HttpClientsFactory());
    }

    @ParameterizedTest
    @ArgumentsSource(AgentTestDataProvider.class)
    void test_getApiObjectMetaData(AgentTestData agentTestData) {
        List<ApiProxyMetaData> apiObjectsMetaData = apiProxyObjectClient.getApiObjectMetaData(agentTestData.createRequestContext());
        log.debug("{} API proxies were found", apiObjectsMetaData.size());

        assertThat(apiObjectsMetaData).isNotEmpty();
    }

    @ParameterizedTest
    @ArgumentsSource(AgentTestDataProvider.class)
    void test_privateApiProxyApiDelete(AgentTestData agentTestData) throws IOException {
        RequestContext requestContext = agentTestData.createRequestContext(agentTestData.getTitle());
        ApiProxyMetaData apiProxyMetaData = getOrCreateDummyApiProxy(requestContext);
        assertThat(apiProxyMetaData).as("Api Poxy %s wasn't found", API_TEST_API_PROXY_NAME).isNotNull();
        apiProxyObjectClient.deleteApiProxy(API_TEST_API_PROXY_NAME, requestContext);
        apiProxyMetaData = apiProxyObjectClient.getApiObjectMetaData(requestContext, API_TEST_API_PROXY_NAME);
        assertThat(apiProxyMetaData).as("Api Poxy %s wasn't deleted", API_TEST_API_PROXY_NAME).isNull();
    }

    private ApiProxyMetaData createDummyApiProxy(RequestContext requestContext) throws IOException {
        byte[] payload = IOUtils.toByteArray(
            this.getClass().getClassLoader().getResource("client/FigafApiTestApiProxy.zip")
        );
        apiProxyObjectClient.uploadApiProxy(requestContext, API_TEST_API_PROXY_NAME, payload);
        return apiProxyObjectClient.getApiObjectMetaData(requestContext, API_TEST_API_PROXY_NAME);
    }

    private ApiProxyMetaData getOrCreateDummyApiProxy(RequestContext requestContext) throws IOException {
        ApiProxyMetaData apiProxyMetaData = apiProxyObjectClient.getApiObjectMetaData(
            requestContext,
            API_TEST_API_PROXY_NAME
        );
        if (apiProxyMetaData == null) {
            apiProxyMetaData = createDummyApiProxy(requestContext);
        }
        return apiProxyMetaData;
    }
}