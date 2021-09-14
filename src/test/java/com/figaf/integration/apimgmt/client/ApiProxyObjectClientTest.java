package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.data_provider.AgentTestDataProvider;
import com.figaf.integration.apimgmt.entity.ApiProxyMetaData;
import com.figaf.integration.common.data_provider.AgentTestData;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilya Nesterov
 */
@Slf4j
class ApiProxyObjectClientTest {

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
}