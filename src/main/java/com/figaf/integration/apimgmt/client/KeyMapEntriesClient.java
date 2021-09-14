package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.entity.KeyMapEntryMetaData;
import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.apimgmt.response_parser.KeyMapEntriesParser;
import com.figaf.integration.common.client.BaseClient;
import com.figaf.integration.common.entity.RequestContext;
import com.figaf.integration.common.exception.ClientIntegrationException;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author Arsenii Istlentev
 * @author Sergey Klochkov
 */
@Slf4j
public class KeyMapEntriesClient extends BaseClient {

    private static final String BATCH_REQUEST = "/apiportal/api/1.0/Management.svc/$batch";
    private static final String KEY_MAP_ENTRIES_WITH_PARAMETERS = "/apiportal/api/1.0/Management.svc/KeyMapEntries?forceUpdateFromRT=true&$format=json";
    private static final String KEY_MAP_ENTRIES = "/apiportal/api/1.0/Management.svc/KeyMapEntries";
    private static final String KEY_MAP_ENTRY_VALUES_WITH_PARAMETERS = "/apiportal/api/1.0/Management.svc/KeyMapEntries('%s')/keyMapEntryValues?forceUpdateFromRT=true&$format=json";
    private static final String KEY_MAP_ENTRY = "/apiportal/api/1.0/Management.svc/KeyMapEntries('%s')?forceUpdateFromRT=true&$format=json";
    private static final String KEY_MAP_ENTRY_VALUES = "/apiportal/api/1.0/Management.svc/KeyMapEntryValues";
    private static final String KEY_MAP_ENTRY_VALUE = "/apiportal/api/1.0/Management.svc/KeyMapEntryValues(map_name='%s',name='%s')";

    // according to HTTP spec https://tools.ietf.org/html/rfc2616#section-2.2, CRLF is a correct line break for HTTP protocol
    private static final String BATCH_REQUEST_LINE_SEPARATOR = "\r\n";
    
    private static final String BATCH_UPDATE_BODY_TEMPLATE = "--%s" + BATCH_REQUEST_LINE_SEPARATOR +
                                                             "Content-Type: multipart/mixed; boundary=%s" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR +
                                                             "%s" +
                                                             "--%s--" + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String BATCH_UPDATE_CHANGE_SET_START_TEMPLATE = "--%s" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                         "Content-Type: application/http" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                         "Content-Transfer-Encoding: binary" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String BATCH_UPDATE_CHANGE_SET_END_TEMPLATE = "--%s--" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String BATCH_UPDATE_CHANGE_SET_BODY_FOR_ADDING_VALUE_TEMPLATE = "POST KeyMapEntryValues HTTP/1.1" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "RequestId: %s" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "Accept-Language: en" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "Accept: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "MaxDataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "DataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "Content-Type: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "Content-Length: %d" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                         "%s" + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String PAYLOAD_FOR_ADDING_VALUE_TEMPLATE = "{\"name\":\"%s\",\"value\":\"%s\",\"map_name\":\"%s\",\"keyMapEntry\":{\"__metadata\":{\"uri\":\"KeyMapEntries('%s')\"}}}";

    private static final String BATCH_UPDATE_CHANGE_SET_BODY_FOR_UPDATING_VALUE_TEMPLATE = "PUT KeyMapEntryValues(map_name='%s',name='%s') HTTP/1.1" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "RequestId: %s" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "Accept-Language: en" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "Accept: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "MaxDataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "DataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "Content-Type: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "Content-Length: %s" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "{\"value\":\"%s\"}" + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String BATCH_UPDATE_CHANGE_SET_BODY_FOR_DELETION_VALUE_TEMPLATE = "DELETE KeyMapEntryValues(map_name='%s',name='%s') HTTP/1.1" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "RequestId: %s" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "Accept-Language: en" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "Accept: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "MaxDataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR +
                                                                                           "DataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR;

    public KeyMapEntriesClient(HttpClientsFactory httpClientsFactory) {
        super(httpClientsFactory);
    }

    public List<String> getKeyMapEntries(RequestContext requestContext) {
        log.debug("#getKeyMapEntries(RequestContext requestContext): {}", requestContext);
        return executeGet(requestContext, KEY_MAP_ENTRIES_WITH_PARAMETERS, KeyMapEntriesParser::buildKeyMapEntryList);
    }

    public List<KeyMapEntryMetaData> getKeyMapEntryMetaDataList(RequestContext requestContext) {
        log.debug("#getKeyMapEntriesList(RequestContext requestContext): {}", requestContext);
        return executeGet(requestContext, KEY_MAP_ENTRIES_WITH_PARAMETERS, KeyMapEntriesParser::buildKeyMapEntryMetaDataList);
    }

    public KeyMapEntryMetaData getKeyMapEntryMetaData(String keyMapEntry, RequestContext requestContext) {
        log.debug("#getKeyMapEntriesList(RequestContext requestContext): {}", requestContext);
        try {
            return executeGet(
                    requestContext,
                    String.format(
                            KEY_MAP_ENTRY,
                            URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20")
                    ),
                    KeyMapEntriesParser::buildKeyMapEntryMetaData
            );
        } catch (UnsupportedEncodingException ex) {
            throw new ClientIntegrationException("Couldn't get key map entry meta data: " + ex.getMessage(), ex);
        }
    }

    public List<KeyMapEntryValue> getKeyMapEntryValues(String keyMapEntry, RequestContext requestContext) {
        log.debug("#getKeyMapEntryValues(String keyMapEntry, RequestContext requestContext): {}, {}", keyMapEntry, requestContext);

        try {
            return executeGet(
                    requestContext,
                    String.format(
                            KEY_MAP_ENTRY_VALUES_WITH_PARAMETERS,
                            URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20")
                    ),
                    body -> KeyMapEntriesParser.buildKeyMapEntryValuesList(keyMapEntry, body)
            );
        } catch (UnsupportedEncodingException ex) {
            throw new ClientIntegrationException("Couldn't get key map entry values: " + ex.getMessage(), ex);
        }
    }

    public Map<String, String> getKeyToValueMap(String keyMapEntry, RequestContext requestContext) {
        log.debug("#getKeyToValueMap(String keyMapEntry, RequestContext requestContext): {}, {}", keyMapEntry, requestContext);
        List<KeyMapEntryValue> keyMapEntryValues = getKeyMapEntryValues(keyMapEntry, requestContext);

        Map<String, String> keyToValueMap = new HashMap<>();
        for (KeyMapEntryValue keyMapEntryValue : keyMapEntryValues) {
            keyToValueMap.put(
                    keyMapEntryValue.getName(),
                    keyMapEntryValue.getValue()
            );
        }

        return keyToValueMap;
    }

    public void createNewKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext) {
        if (!keyMapEntryMetaData.isEncrypted()) {
            log.debug("#createNewKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext): {}, {}",
                    keyMapEntryMetaData, requestContext);
        } else {
            log.debug("#createNewKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext): {}, {}",
                    keyMapEntryMetaData.getName(), requestContext);
        }

        executeMethod(
                requestContext,
                KEY_MAP_ENTRY_VALUES,
                KEY_MAP_ENTRIES,
                (url, token, restTemplateWrapper) -> {
                    createNewKeyMapEntry(keyMapEntryMetaData, url, token, restTemplateWrapper.getRestTemplate());
                    return null;
                }
        );
    }

    public void updateKeyMapEntry(String keyMapEntry, Map<String, String> keyToValueMap, RequestContext requestContext) {
        log.debug("#updateKeyMapEntry(String keyMapEntry, Map<String, String> keyToValueMap, RequestContext requestContext): {}, {}",
                keyMapEntry, requestContext);

        List<String> keyMapEntries = getKeyMapEntries(requestContext);

        if (!keyMapEntries.contains(keyMapEntry)) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't update key map entry %s, because it's not exist",
                    keyMapEntry
            ));
        }

        executeMethod(
                requestContext,
                KEY_MAP_ENTRY_VALUES,
                BATCH_REQUEST,
                (url, token, restTemplateWrapper) -> {
                    try {
                        updateKeyMapEntry(
                                keyMapEntry,
                                keyToValueMap,
                                getKeyToValueMap(keyMapEntry, requestContext),
                                url,
                                token,
                                restTemplateWrapper.getRestTemplate()
                        );
                        return null;
                    } catch (UnsupportedEncodingException ex) {
                        throw new ClientIntegrationException("Couldn't update key map entry: " + ex.getMessage(), ex);
                    }
                }
        );
    }

    public void createOrUpdateKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext) {
        if (!keyMapEntryMetaData.isEncrypted()) {
            log.debug("#createOrUpdateKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext): {}, {}",
                    keyMapEntryMetaData, requestContext);
        } else {
            log.debug("#createOrUpdateKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext): {}, {}",
                    keyMapEntryMetaData.getName(), requestContext);
        }

        List<String> keyMapEntries = getKeyMapEntries(requestContext);

        if (!keyMapEntries.contains(keyMapEntryMetaData.getName())) {

            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    KEY_MAP_ENTRIES,
                    (url, token, restTemplateWrapper) -> {
                        createNewKeyMapEntry(keyMapEntryMetaData, url, token, restTemplateWrapper.getRestTemplate());
                        return null;
                    }
            );

        } else {

            Map<String, String> remoteKeyToValueMap = getKeyToValueMap(keyMapEntryMetaData.getName(), requestContext);

            Map<String, String> keyToValueMap = new HashMap<>();
            for (KeyMapEntryValue keyMapEntryValue : keyMapEntryMetaData.getKeyMapEntryValues()) {
                keyToValueMap.put(keyMapEntryValue.getName(), keyMapEntryValue.getValue());
            }

            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    BATCH_REQUEST,
                    (url, token, restTemplateWrapper) -> {
                        try {
                            updateKeyMapEntry(
                                    keyMapEntryMetaData.getName(),
                                    keyToValueMap,
                                    remoteKeyToValueMap,
                                    url,
                                    token,
                                    restTemplateWrapper.getRestTemplate()
                            );
                            return null;
                        } catch (UnsupportedEncodingException ex) {
                            throw new ClientIntegrationException("Couldn't update key map entry: " + ex.getMessage(), ex);
                        }
                    }
            );

        }
    }

    public void addKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String keyMapEntryValue, RequestContext requestContext) {
        log.debug("#addKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String keyMapEntryValue, RequestContext requestContext): {}, {}, {}",
                keyMapEntry, keyMapEntryValueName, requestContext);

        executeMethod(
                requestContext,
                KEY_MAP_ENTRY_VALUES,
                KEY_MAP_ENTRY_VALUES,
                (url, token, restTemplateWrapper) -> {
                    addKeyMapEntryValue(
                            keyMapEntry,
                            keyMapEntryValueName,
                            keyMapEntryValue,
                            url,
                            token,
                            restTemplateWrapper.getRestTemplate()
                    );
                    return null;
                }
        );

    }

    public void updateKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String newKeyMapEntryValue, RequestContext requestContext) {
        log.debug("#updateKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String newKeyMapEntryValue, RequestContext requestContext): {}, {}, {}",
                keyMapEntry, keyMapEntryValueName, requestContext);

        executeMethod(
                requestContext,
                KEY_MAP_ENTRY_VALUES,
                String.format(KEY_MAP_ENTRY_VALUE, keyMapEntry, keyMapEntryValueName),
                (url, token, restTemplateWrapper) -> {
                    updateKeyMapEntryValue(
                            keyMapEntry,
                            keyMapEntryValueName,
                            newKeyMapEntryValue,
                            url,
                            token,
                            restTemplateWrapper.getRestTemplate()
                    );
                    return null;
                }
        );

    }

    public void deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, RequestContext requestContext) {
        log.debug("#deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, RequestContext requestContext): {}, {}, {}",
                keyMapEntry, keyMapEntryValueName, requestContext);

        executeMethod(
                requestContext,
                KEY_MAP_ENTRY_VALUES,
                String.format(KEY_MAP_ENTRY_VALUE, keyMapEntry, keyMapEntryValueName),
                (url, token, restTemplateWrapper) -> {
                    deleteKeyMapEntryValue(keyMapEntry, keyMapEntryValueName, url, token, restTemplateWrapper.getRestTemplate());
                    return null;
                }
        );

    }

    private void createNewKeyMapEntry(
            KeyMapEntryMetaData keyMapEntryMetaData,
            String url,
            String token,
            RestTemplate restTemplate
    ) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);

        HttpEntity<KeyMapEntryMetaData> httpEntity = new HttpEntity<>(keyMapEntryMetaData, httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);

        if (!HttpStatus.CREATED.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't create key map entry %s: Code: %d, Message: %s",
                    keyMapEntryMetaData.getName(),
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private void updateKeyMapEntry(
            String keyMapEntry,
            Map<String, String> keyToValueMap,
            Map<String, String> remoteKeyToValueMap,
            String url,
            String token,
            RestTemplate restTemplate
    ) throws UnsupportedEncodingException {

        Map<String, String> valuesForAdding = new HashMap<>();
        Map<String, String> valuesForUpdating = new HashMap<>();

        for (Map.Entry<String, String> keyToValueMapEntry : keyToValueMap.entrySet()) {

            if (!remoteKeyToValueMap.containsKey(keyToValueMapEntry.getKey())) {
                valuesForAdding.put(keyToValueMapEntry.getKey(), keyToValueMapEntry.getValue());
            }

            if (remoteKeyToValueMap.containsKey(keyToValueMapEntry.getKey()) &&
                    !remoteKeyToValueMap.get(keyToValueMapEntry.getKey()).equals(keyToValueMapEntry.getValue())) {
                valuesForUpdating.put(keyToValueMapEntry.getKey(), keyToValueMapEntry.getValue());
            }

            remoteKeyToValueMap.remove(keyToValueMapEntry.getKey());
        }

        Map<String, String> valuesForDeletion = new HashMap<>(remoteKeyToValueMap);

        if (CollectionUtils.isEmpty(valuesForAdding.entrySet()) &&
            CollectionUtils.isEmpty(valuesForUpdating.entrySet()) &&
            CollectionUtils.isEmpty(valuesForDeletion.entrySet())
        ) {
            return;
        }

        StringBuilder changeSets = new StringBuilder();
        String changeSetSeparator = String.format("changeset_%s", UUID.randomUUID());
        String requestId = UUID.randomUUID().toString();

        for (Map.Entry<String, String> valueForAdding : valuesForAdding.entrySet()) {
            changeSets.append(String.format(BATCH_UPDATE_CHANGE_SET_START_TEMPLATE, changeSetSeparator));

            String payload = String.format(
                    PAYLOAD_FOR_ADDING_VALUE_TEMPLATE,
                    valueForAdding.getKey(),
                    valueForAdding.getValue(),
                    keyMapEntry,
                    URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20")
            );

            changeSets.append(String.format(
                    BATCH_UPDATE_CHANGE_SET_BODY_FOR_ADDING_VALUE_TEMPLATE,
                    requestId,
                    payload.length(),
                    payload
            ));
        }

        for (Map.Entry<String, String> valueForUpdating : valuesForUpdating.entrySet()) {
            changeSets.append(String.format(BATCH_UPDATE_CHANGE_SET_START_TEMPLATE, changeSetSeparator));

            changeSets.append(String.format(
                    BATCH_UPDATE_CHANGE_SET_BODY_FOR_UPDATING_VALUE_TEMPLATE,
                    URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    URLEncoder.encode(valueForUpdating.getKey(), StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    requestId,
                    valueForUpdating.getValue().length() + 12,
                    valueForUpdating.getValue()
            ));
        }

        for (Map.Entry<String, String> valueForDeletion : valuesForDeletion.entrySet()) {
            changeSets.append(String.format(BATCH_UPDATE_CHANGE_SET_START_TEMPLATE, changeSetSeparator));

            changeSets.append(String.format(
                    BATCH_UPDATE_CHANGE_SET_BODY_FOR_DELETION_VALUE_TEMPLATE,
                    URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    URLEncoder.encode(valueForDeletion.getKey(), StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    requestId
            ));
        }

        changeSets.append(String.format(BATCH_UPDATE_CHANGE_SET_END_TEMPLATE, changeSetSeparator));

        String bodySeparator = String.format("batch_%s", UUID.randomUUID());
        String body = String.format(
                BATCH_UPDATE_BODY_TEMPLATE,
                bodySeparator,
                changeSetSeparator,
                changeSets.toString(),
                bodySeparator
        );

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        httpHeaders.add("Content-Type", String.format("multipart/mixed;boundary=%s", bodySeparator));

        HttpEntity<String> httpEntity = new HttpEntity<>(body, httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);

        if (!HttpStatus.ACCEPTED.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't update key map entry %s: Code: %d, Message: %s",
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private void addKeyMapEntryValue(
            String keyMapEntry,
            String keyMapEntryValueName,
            String keyMapEntryValue,
            String url,
            String token,
            RestTemplate restTemplate
    ) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);

        String body = String.format(
                "{" +
                "  \"map_name\": \"%s\"," +
                "  \"name\": \"%s\"," +
                "  \"value\": \"%s\"," +
                "  \"keyMapEntry\": [" +
                "    {" +
                "      \"__metadata\": {" +
                "        \"uri\": \"KeyMapEntries('%s')\"" +
                "      }" +
                "    }" +
                "  ]" +
                "}",
                keyMapEntry,
                keyMapEntryValueName,
                keyMapEntryValue,
                keyMapEntry
        );

        HttpEntity<String> httpEntity = new HttpEntity<>(body, httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);

        if (!HttpStatus.CREATED.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't create key value entry %s in key map entry %s: Code: %d, Message: %s",
                    keyMapEntryValueName,
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private void updateKeyMapEntryValue(
            String keyMapEntry,
            String keyMapEntryValueName,
            String keyMapEntryValue,
            String url,
            String token,
            RestTemplate restTemplate
    ) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);

        String body = String.format(
                "{" +
                "  \"value\": \"%s\"" +
                "}",
                keyMapEntryValue
        );

        HttpEntity<String> httpEntity = new HttpEntity<>(body, httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.PUT, httpEntity, String.class);

        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't update key value entry %s in key map entry %s: Code: %d, Message: %s",
                    keyMapEntryValueName,
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private void deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String url, String token, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        HttpEntity<Void> httpEntity = new HttpEntity<>(httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.DELETE, httpEntity, String.class);

        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't delete key map entry value %s in key map entry %s: Code: %d, Message: %s",
                    keyMapEntryValueName,
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

}
