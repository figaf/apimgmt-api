package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.data_provider.AgentTestDataProvider;
import com.figaf.integration.apimgmt.entity.KeyMapEntryMetaData;
import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.common.data_provider.AgentTestData;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
        keyMapEntriesClient = new KeyMapEntriesClient(new HttpClientsFactory());
    }

    @ParameterizedTest
    @ArgumentsSource(AgentTestDataProvider.class)
    void test_getKeyMapEntries(AgentTestData agentTestData) {
        List<String> keyMapEntries = keyMapEntriesClient.getKeyMapEntries(agentTestData.createRequestContext());
        log.debug("{} KeyMap entries were found", keyMapEntries.size());

        assertThat(keyMapEntries).isNotEmpty();
    }

    // was used for debugging purpose
    @Disabled
    @ParameterizedTest
    @ArgumentsSource(AgentTestDataProvider.class)
    void test_createOrUpdateKeyMapEntry(AgentTestData agentTestData) {
        KeyMapEntryMetaData keyMapEntryMetaData = keyMapEntriesClient.getKeyMapEntryMetaData("encrypt2KVM", agentTestData.createRequestContext());
        List<KeyMapEntryValue> keyMapEntryValues = keyMapEntriesClient.getKeyMapEntryValues("encrypt2KVM", agentTestData.createRequestContext());
        keyMapEntryValues.get(0).setValue("value");
        keyMapEntryValues.get(1).setValue("value");
        keyMapEntryMetaData.setKeyMapEntryValues(keyMapEntryValues);
        keyMapEntriesClient.createOrUpdateKeyMapEntry(keyMapEntryMetaData, agentTestData.createRequestContext());
    }

}