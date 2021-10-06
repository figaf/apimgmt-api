package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.data_provider.AgentTestDataProvider;
import com.figaf.integration.apimgmt.entity.KeyMapEntryMetaData;
import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.common.data_provider.AgentTestData;
import com.figaf.integration.common.entity.RequestContext;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilya Nesterov
 */
@Slf4j
class KeyMapEntriesClientTest {

    private static final String API_TEST_KEY_MAP_ENTRY_NAME = "FigafApiTestKeyMapEntry";

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

    @ParameterizedTest
    @ArgumentsSource(AgentTestDataProvider.class)
    void test_privateKeyMapEntryApiDelete(AgentTestData agentTestData) {
        RequestContext requestContext = agentTestData.createRequestContext(agentTestData.getTitle());
        KeyMapEntryMetaData keyMapEntryMetaData = getOrCreateDummyKeyMapEntry(requestContext);
        assertThat(keyMapEntryMetaData).as("Key map entry %s wasn't found", API_TEST_KEY_MAP_ENTRY_NAME).isNotNull();
        keyMapEntriesClient.deleteKeyMapEntry(API_TEST_KEY_MAP_ENTRY_NAME, requestContext);
        keyMapEntryMetaData = keyMapEntriesClient.getKeyMapEntryMetaData(API_TEST_KEY_MAP_ENTRY_NAME, requestContext);
        assertThat(keyMapEntryMetaData).as("Key map entry %s wasn't deleted", API_TEST_KEY_MAP_ENTRY_NAME).isNull();
    }

    private KeyMapEntryMetaData createDummyKeyMapEntry(RequestContext requestContext) {
        KeyMapEntryMetaData keyMapEntryMetaData = new KeyMapEntryMetaData();
        keyMapEntryMetaData.setName(API_TEST_KEY_MAP_ENTRY_NAME);
        keyMapEntryMetaData.setEncrypted(false);
        keyMapEntryMetaData.setScope("ENV");

        keyMapEntryMetaData.setKeyMapEntryValues(new ArrayList<>());
        KeyMapEntryValue keyMapEntryValue = new KeyMapEntryValue(
            API_TEST_KEY_MAP_ENTRY_NAME,
            "key",
            "value"
        );
        keyMapEntryMetaData.getKeyMapEntryValues().add(keyMapEntryValue);
        keyMapEntriesClient.createOrUpdateKeyMapEntry(keyMapEntryMetaData, requestContext);
        return keyMapEntriesClient.getKeyMapEntryMetaData(API_TEST_KEY_MAP_ENTRY_NAME, requestContext);
    }

    private KeyMapEntryMetaData getOrCreateDummyKeyMapEntry(RequestContext requestContext) {
        KeyMapEntryMetaData keyMapEntryMetaData = keyMapEntriesClient.getKeyMapEntryMetaData(
            API_TEST_KEY_MAP_ENTRY_NAME,
            requestContext
        );
        if (keyMapEntryMetaData == null) {
            keyMapEntryMetaData = createDummyKeyMapEntry(requestContext);
        }
        return keyMapEntryMetaData;
    }

}