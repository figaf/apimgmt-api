package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.data_provider.AgentTestDataProvider;
import com.figaf.integration.common.data_provider.AgentTestData;
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
class KeyMapEntriesClientTest {

    private static KeyMapEntriesClient keyMapEntriesClient;

    @BeforeAll
    static void setUp() {
        keyMapEntriesClient = new KeyMapEntriesClient("https://accounts.sap.com/saml2/idp/sso");
    }

    @ParameterizedTest
    @ArgumentsSource(AgentTestDataProvider.class)
    void test_getKeyMapEntries(AgentTestData agentTestData) {
        List<String> keyMapEntries = keyMapEntriesClient.getKeyMapEntries(agentTestData.createRequestContext());
        log.debug("{} KeyMap entries were found", keyMapEntries.size());

        assertThat(keyMapEntries).isNotEmpty();
    }

}