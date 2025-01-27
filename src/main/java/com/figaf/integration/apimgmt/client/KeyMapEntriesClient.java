package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.entity.KeyMapEntryMetaData;
import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.apimgmt.response_parser.KeyMapEntriesParser;
import com.figaf.integration.common.client.BaseClient;
import com.figaf.integration.common.entity.RequestContext;
import com.figaf.integration.common.exception.ClientIntegrationException;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.figaf.integration.common.entity.AuthenticationType.OAUTH;
import static java.lang.String.format;
import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;

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
    private static final String KEY_MAP_ENTRIES_WITH_NAME = "/apiportal/api/1.0/Management.svc/KeyMapEntries('%s')";

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

    private static final String COMMON_PART_OF_BODY = "RequestId: %s" + BATCH_REQUEST_LINE_SEPARATOR +
            "Accept-Language: en" + BATCH_REQUEST_LINE_SEPARATOR +
            "Accept: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
            "MaxDataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR +
            "DataServiceVersion: 2.0" + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String BATCH_UPDATE_CHANGE_SET_BODY_FOR_ADDING_VALUE_TEMPLATE = "POST KeyMapEntryValues HTTP/1.1" + BATCH_REQUEST_LINE_SEPARATOR +
            COMMON_PART_OF_BODY +
            "Content-Type: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
            "Content-Length: %d" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR +
            "%s" + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String PAYLOAD_FOR_ADDING_VALUE_TEMPLATE = "{\"name\":\"%s\",\"value\":\"%s\",\"map_name\":\"%s\",\"keyMapEntry\":{\"__metadata\":{\"uri\":\"KeyMapEntries('%s')\"}}}";

    private static final String BATCH_UPDATE_CHANGE_SET_BODY_FOR_UPDATING_VALUE_TEMPLATE = "PUT KeyMapEntryValues(map_name='%s',name='%s') HTTP/1.1" + BATCH_REQUEST_LINE_SEPARATOR +
            COMMON_PART_OF_BODY +
            "Content-Type: application/json" + BATCH_REQUEST_LINE_SEPARATOR +
            "Content-Length: %s" + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR +
            "{\"value\":\"%s\"}" + BATCH_REQUEST_LINE_SEPARATOR;

    private static final String BATCH_UPDATE_CHANGE_SET_BODY_FOR_DELETION_VALUE_TEMPLATE = "DELETE KeyMapEntryValues(map_name='%s',name='%s') HTTP/1.1" + BATCH_REQUEST_LINE_SEPARATOR +
            COMMON_PART_OF_BODY + BATCH_REQUEST_LINE_SEPARATOR + BATCH_REQUEST_LINE_SEPARATOR;

    public KeyMapEntriesClient(HttpClientsFactory httpClientsFactory) {
        super(httpClientsFactory);
    }

    public List<String> getKeyMapEntries(RequestContext requestContext) {
        log.debug("#getKeyMapEntries(RequestContext requestContext): {}", requestContext);
        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            return executeGetPublicApiAndReturnResponseBody(requestContext, KEY_MAP_ENTRIES_WITH_PARAMETERS, KeyMapEntriesParser::buildKeyMapEntryList);
        }
        return executeGet(requestContext, KEY_MAP_ENTRIES_WITH_PARAMETERS, KeyMapEntriesParser::buildKeyMapEntryList);
    }

    public List<KeyMapEntryMetaData> getKeyMapEntryMetaDataList(RequestContext requestContext) {
        log.debug("#getKeyMapEntriesList(RequestContext requestContext): {}", requestContext);
        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            return executeMethodPublicApi(
                    requestContext,
                    KEY_MAP_ENTRIES_WITH_PARAMETERS,
                    null,
                    HttpMethod.GET,
                    x -> KeyMapEntriesParser.buildKeyMapEntryMetaDataList(x.getBody())
            );
        }
        return executeGet(
                requestContext,
                KEY_MAP_ENTRIES_WITH_PARAMETERS,
                KeyMapEntriesParser::buildKeyMapEntryMetaDataList
        );
    }

    public KeyMapEntryMetaData getKeyMapEntryMetaData(String keyMapEntry, RequestContext requestContext) {
        log.debug("#getKeyMapEntryMetaData(RequestContext requestContext): {}", requestContext);
        KeyMapEntryMetaData keyMapEntryMetaData = null;
        try {
            String encodedEntry = URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20");
            String url = format(KEY_MAP_ENTRY, encodedEntry);
            if (OAUTH.equals(requestContext.getAuthenticationType())) {
                keyMapEntryMetaData = executeMethodPublicApi(
                        requestContext,
                        url,
                        null,
                        HttpMethod.GET,
                        x -> KeyMapEntriesParser.buildKeyMapEntryMetaData(x.getBody())
                );
            } else {
                keyMapEntryMetaData = executeGet(requestContext, url, KeyMapEntriesParser::buildKeyMapEntryMetaData);
            }
        } catch (UnsupportedEncodingException ex) {
            throw new ClientIntegrationException("Couldn't get key map entry meta data: " + ex.getMessage(), ex);
        } catch (HttpStatusCodeException ex) {
            //this case happens when we try to get non existing object on cloud foundry system and
            //we didn't make Auth request for current 'restTemplateWrapperKey' before
            if (!NOT_FOUND.equals(ex.getStatusCode())) {
                throw ex;
            }
        } catch (ClientIntegrationException ex) {
            //this case happens when we try to get non existing object on cloud foundry system and
            //we made Auth request for current 'restTemplateWrapperKey' before
            if (!(ex.getCause() instanceof HttpStatusCodeException) ||
                    !NOT_FOUND.equals(((HttpStatusCodeException) ex.getCause()).getStatusCode())
            ) {
                throw ex;
            }
        }
        return keyMapEntryMetaData;
    }

    public List<KeyMapEntryValue> getKeyMapEntryValues(String keyMapEntry, RequestContext requestContext) {
        log.debug("#getKeyMapEntryValues(String keyMapEntry, RequestContext requestContext): {}, {}", keyMapEntry, requestContext);

        try {
            String encodedKeyMapEntry = URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20");
            String path = format(KEY_MAP_ENTRY_VALUES_WITH_PARAMETERS, encodedKeyMapEntry);
            if (OAUTH.equals(requestContext.getAuthenticationType())) {
                return executeMethodPublicApi(
                        requestContext,
                        path,
                        null,
                        HttpMethod.GET,
                        response -> KeyMapEntriesParser.buildKeyMapEntryValuesList(keyMapEntry, response.getBody())
                );
            }
            return executeGet(
                    requestContext,
                    format(
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

        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            executeMethodPublicApi(
                    requestContext,
                    KEY_MAP_ENTRIES,
                    keyMapEntryMetaData,
                    HttpMethod.POST,
                    response -> {
                        if (!HttpStatus.CREATED.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(String.format(
                                    "Couldn't create key map entry %s: Code: %d, Message: %s",
                                    keyMapEntryMetaData.getName(),
                                    response.getStatusCode().value(),
                                    response.getBody())
                            );
                        }
                        return null;
                    }
            );
        } else {
            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    KEY_MAP_ENTRIES,
                    (url, token, restTemplateWrapper) -> {
                        createNewKeyMapEntryMetadata(keyMapEntryMetaData, url, token, restTemplateWrapper.getRestTemplate());
                        return null;
                    }
            );
        }
    }

    public void deleteKeyMapEntry(String keyMapEntryId, RequestContext requestContext) {
        log.debug("#deleteKeyMapEntry(String keyMapEntryId, RequestContext requestContext): {}, {}", keyMapEntryId, requestContext);
        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            executeDeletePublicApi(
                    requestContext,
                    format(KEY_MAP_ENTRIES_WITH_NAME, keyMapEntryId),
                    response -> {
                        if (!HttpStatus.NO_CONTENT.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(format(
                                    "Couldn't delete key map entry %s: Code: %d, Message: %s",
                                    keyMapEntryId,
                                    response.getStatusCode().value(),
                                    response.getBody())
                            );
                        }
                        return null;
                    }
            );
        } else {
            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    format(KEY_MAP_ENTRIES_WITH_NAME, keyMapEntryId),
                    (url, token, restTemplateWrapper) -> {
                        deleteKeyMapEntry(keyMapEntryId, url, token, restTemplateWrapper.getRestTemplate());
                        return null;
                    }
            );
        }
    }

    public void updateKeyMapEntry(String keyMapEntry, Map<String, String> keyToValueMap, RequestContext requestContext) {
        log.debug("#updateKeyMapEntry(String keyMapEntry, Map<String, String> keyToValueMap, RequestContext requestContext): {}, {}",
                keyMapEntry, requestContext);

        List<String> keyMapEntries = getKeyMapEntries(requestContext);

        if (!keyMapEntries.contains(keyMapEntry)) {
            throw new ClientIntegrationException(format(
                    "Couldn't update key map entry %s, because it's not exist",
                    keyMapEntry
            ));
        }

        Map<String, String> remoteKeyToValueMap = getKeyToValueMap(keyMapEntry, requestContext);

        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            String bodySeparator = format("batch_%s", UUID.randomUUID());
            Optional<String> requestBody = prepareRequestBodyForUpdatingKeyMap(
                    keyMapEntry,
                    bodySeparator,
                    keyToValueMap,
                    remoteKeyToValueMap
            );
            if (!requestBody.isPresent()) {
                return;
            }
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.add("Content-Type", format("multipart/mixed;boundary=%s", bodySeparator));

            executeMethodPublicApiWithCustomHeaders(
                    requestContext,
                    BATCH_REQUEST,
                    requestBody.get(),
                    HttpMethod.POST,
                    httpHeaders,
                    response -> {
                        if (!HttpStatus.ACCEPTED.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(format(
                                    "Couldn't update key map entry %s: Code: %d, Message: %s",
                                    keyMapEntry,
                                    response.getStatusCode().value(),
                                    response.getBody())
                            );
                        }
                        return null;
                    }
            );
        } else {
            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    BATCH_REQUEST,
                    (url, token, restTemplateWrapper) -> {
                        updateKeyMapEntry(
                                keyMapEntry,
                                keyToValueMap,
                                remoteKeyToValueMap,
                                url,
                                token,
                                restTemplateWrapper.getRestTemplate()
                        );
                        return null;
                    }
            );
        }
    }

    public void createOrUpdateKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext) {
        String keyMapEntry = keyMapEntryMetaData.getName();
        if (!keyMapEntryMetaData.isEncrypted()) {
            log.debug("#createOrUpdateKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext): {}, {}",
                    keyMapEntryMetaData, requestContext);
        } else {
            log.debug("#createOrUpdateKeyMapEntry(KeyMapEntryMetaData keyMapEntryMetaData, RequestContext requestContext): {}, {}",
                    keyMapEntry, requestContext);
        }
        List<String> keyMapEntries = getKeyMapEntries(requestContext);
        if (!keyMapEntries.contains(keyMapEntry)) {
            createNewKeyMapEntry(keyMapEntryMetaData, requestContext);
        } else {
            Map<String, String> keyToValueMap = new HashMap<>();
            List<KeyMapEntryValue> keyMapEntryValues = keyMapEntryMetaData.getKeyMapEntryValues();
            if (CollectionUtils.isEmpty(keyMapEntryValues)) {
                return;
            }
            for (KeyMapEntryValue keyMapEntryValue : keyMapEntryValues) {
                keyToValueMap.put(keyMapEntryValue.getName(), keyMapEntryValue.getValue());
            }
            updateKeyMapEntry(keyMapEntry, keyToValueMap, requestContext);
        }
    }

    public void createOrUpdateKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String newKeyMapEntryValue, RequestContext requestContext) {
        List<KeyMapEntryValue> keyMapEntryValues = getKeyMapEntryValues(keyMapEntry, requestContext);
        for (KeyMapEntryValue currentKeyMapEntryValue : keyMapEntryValues) {
            if (currentKeyMapEntryValue.getName().equals(keyMapEntryValueName)) {
                if (!newKeyMapEntryValue.equals(currentKeyMapEntryValue.getValue())) {
                    updateKeyMapEntryValue(keyMapEntry, keyMapEntryValueName, newKeyMapEntryValue, requestContext);
                }
                return;
            }
        }

        addKeyMapEntryValue(keyMapEntry, keyMapEntryValueName, newKeyMapEntryValue, requestContext);
    }

    public void addKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String keyMapEntryValue, RequestContext requestContext) {
        log.debug("#addKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String keyMapEntryValue, RequestContext requestContext): {}, {}, {}",
                keyMapEntry, keyMapEntryValueName, requestContext);

        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            String requestBody = prepareRequestBodyForNewEntryValue(keyMapEntry, keyMapEntryValueName, keyMapEntryValue);
            executeMethodPublicApi(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    requestBody,
                    HttpMethod.POST,
                    response -> {
                        if (!HttpStatus.CREATED.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(format(
                                    "Couldn't create key value entry %s in key map entry %s: Code: %d, Message: %s",
                                    keyMapEntryValueName,
                                    keyMapEntry,
                                    response.getStatusCode().value(),
                                    response.getBody())
                            );
                        }
                        return null;
                    }
            );
        } else {
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
    }

    public void updateKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String newKeyMapEntryValue, RequestContext requestContext) {
        log.debug("#updateKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String newKeyMapEntryValue, RequestContext requestContext): {}, {}, {}",
                keyMapEntry, keyMapEntryValueName, requestContext);

        String pathForMainRequest = format(KEY_MAP_ENTRY_VALUE, keyMapEntry, keyMapEntryValueName);

        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            String body = format("{ \"value\": \"%s\" }", newKeyMapEntryValue);
            executeMethodPublicApi(
                    requestContext,
                    pathForMainRequest,
                    body,
                    HttpMethod.PUT,
                    response -> {
                        if (!HttpStatus.NO_CONTENT.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(format(
                                    "Couldn't update key value entry %s in key map entry %s: Code: %d, Message: %s",
                                    keyMapEntryValueName,
                                    keyMapEntry,
                                    response.getStatusCode().value(),
                                    response.getBody())
                            );
                        }
                        return null;
                    }
            );
        } else {
            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    pathForMainRequest,
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
    }

    public void deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, RequestContext requestContext) {
        log.debug("#deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, RequestContext requestContext): {}, {}, {}",
                keyMapEntry, keyMapEntryValueName, requestContext);
        String pathForMainRequest = format(KEY_MAP_ENTRY_VALUE, keyMapEntry, keyMapEntryValueName);

        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            executeDeletePublicApi(
                    requestContext,
                    pathForMainRequest,
                    response -> {
                        if (!HttpStatus.NO_CONTENT.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(format(
                                    "Couldn't delete key map entry value %s in key map entry %s: Code: %d, Message: %s",
                                    keyMapEntryValueName,
                                    keyMapEntry,
                                    response.getStatusCode().value(),
                                    response.getBody())
                            );
                        }
                        return null;
                    }
            );
        } else {
            executeMethod(
                    requestContext,
                    KEY_MAP_ENTRY_VALUES,
                    pathForMainRequest,
                    (url, token, restTemplateWrapper) -> {
                        deleteKeyMapEntryValue(keyMapEntry, keyMapEntryValueName, url, token, restTemplateWrapper.getRestTemplate());
                        return null;
                    }
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
        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        httpHeaders.setContentType(APPLICATION_JSON_UTF8);

        String body = format(
                "{" +
                        "  \"value\": \"%s\"" +
                        "}",
                keyMapEntryValue
        );

        HttpEntity<String> httpEntity = new HttpEntity<>(body, httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.PUT, httpEntity, String.class);

        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(format(
                    "Couldn't update key value entry %s in key map entry %s: Code: %d, Message: %s",
                    keyMapEntryValueName,
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private void deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String url, String token, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        HttpEntity<Void> httpEntity = new HttpEntity<>(httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, DELETE, httpEntity, String.class);

        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(format(
                    "Couldn't delete key map entry value %s in key map entry %s: Code: %d, Message: %s",
                    keyMapEntryValueName,
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
        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        httpHeaders.setContentType(APPLICATION_JSON_UTF8);
        String body = prepareRequestBodyForNewEntryValue(keyMapEntry, keyMapEntryValueName, keyMapEntryValue);
        HttpEntity<String> httpEntity = new HttpEntity<>(body, httpHeaders);
        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);

        if (!HttpStatus.CREATED.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(format(
                    "Couldn't create key value entry %s in key map entry %s: Code: %d, Message: %s",
                    keyMapEntryValueName,
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private String prepareRequestBodyForNewEntryValue(String keyMapEntry, String keyMapEntryValueName, String keyMapEntryValue) {
        return format(
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
    }

    private void createNewKeyMapEntryMetadata(
            KeyMapEntryMetaData keyMapEntryMetaData,
            String url,
            String token,
            RestTemplate restTemplate
    ) {
        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        httpHeaders.setContentType(APPLICATION_JSON_UTF8);

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

    private void deleteKeyMapEntry(
            String keyMapEntryId,
            String url,
            String token,
            RestTemplate restTemplate
    ) {
        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        HttpEntity<Void> httpEntity = new HttpEntity<>(httpHeaders);
        ResponseEntity<String> responseEntity = restTemplate.exchange(url, DELETE, httpEntity, String.class);

        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(format(
                    "Couldn't delete key map entry %s: Code: %d, Message: %s",
                    keyMapEntryId,
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
    ) {
        String bodySeparator = format("batch_%s", UUID.randomUUID());
        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        httpHeaders.add("Content-Type", format("multipart/mixed;boundary=%s", bodySeparator));
        Optional<String> body = prepareRequestBodyForUpdatingKeyMap(keyMapEntry, bodySeparator, keyToValueMap, remoteKeyToValueMap);
        if (!body.isPresent()) {
            return;
        }
        HttpEntity<String> httpEntity = new HttpEntity<>(body.get(), httpHeaders);
        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);
        if (!HttpStatus.ACCEPTED.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(format(
                    "Couldn't update key map entry %s: Code: %d, Message: %s",
                    keyMapEntry,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

    private Optional<String> prepareRequestBodyForUpdatingKeyMap(
            String keyMapEntry,
            String bodySeparator,
            Map<String, String> keyToValueMap,
            Map<String, String> remoteKeyToValueMap
    ) {

        Map<String, String> valuesForAdding = new HashMap<>();
        Map<String, String> valuesForUpdating = new HashMap<>();
        Map<String, String> valuesForDeletion = new HashMap<>();

        discoverValuesToBeChanged(
                keyToValueMap,
                remoteKeyToValueMap,
                valuesForAdding,
                valuesForUpdating,
                valuesForDeletion
        );

        if (valuesForAdding.isEmpty() && valuesForUpdating.isEmpty() && valuesForDeletion.isEmpty()) {
            return Optional.empty();
        }

        String changeSetSeparator = format("changeset_%s", UUID.randomUUID());
        String requestId = UUID.randomUUID().toString();
        StringBuilder changeSets = new StringBuilder();

        try {
            addChangeSetsForAddedValues(changeSets, keyMapEntry, valuesForAdding, changeSetSeparator, requestId);
            addChangeSetsForUpdatedValues(changeSets, keyMapEntry, valuesForUpdating, changeSetSeparator, requestId);
            addChangeSetsForDeletedValues(changeSets, keyMapEntry, valuesForDeletion, changeSetSeparator, requestId);
        } catch (UnsupportedEncodingException ex) {
            throw new ClientIntegrationException("Couldn't get key map entry values: " + ex.getMessage(), ex);
        }

        changeSets.append(format(BATCH_UPDATE_CHANGE_SET_END_TEMPLATE, changeSetSeparator));
        String body = format(
                BATCH_UPDATE_BODY_TEMPLATE,
                bodySeparator,
                changeSetSeparator,
                changeSets,
                bodySeparator
        );

        return Optional.of(body);
    }

    private static void discoverValuesToBeChanged(
            Map<String, String> keyToValueMap,
            Map<String, String> remoteKeyToValueMap,
            Map<String, String> valuesForAdding,
            Map<String, String> valuesForUpdating,
            Map<String, String> valuesForDeletion
    ) {
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

        valuesForDeletion.putAll(remoteKeyToValueMap);
    }

    private static void addChangeSetsForDeletedValues(
            StringBuilder changeSets,
            String keyMapEntry,
            Map<String, String> valuesForDeletion,
            String changeSetSeparator,
            String requestId
    ) throws UnsupportedEncodingException {
        for (Map.Entry<String, String> valueForDeletion : valuesForDeletion.entrySet()) {
            changeSets.append(format(BATCH_UPDATE_CHANGE_SET_START_TEMPLATE, changeSetSeparator));
            changeSets.append(format(
                    BATCH_UPDATE_CHANGE_SET_BODY_FOR_DELETION_VALUE_TEMPLATE,
                    URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    URLEncoder.encode(valueForDeletion.getKey(), StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    requestId
            ));
        }
    }

    private void addChangeSetsForUpdatedValues(
            StringBuilder changeSets,
            String keyMapEntry,
            Map<String, String> valuesForUpdating,
            String changeSetSeparator,
            String requestId
    ) throws UnsupportedEncodingException {
        for (Map.Entry<String, String> valueForUpdating : valuesForUpdating.entrySet()) {
            changeSets.append(format(BATCH_UPDATE_CHANGE_SET_START_TEMPLATE, changeSetSeparator));

            changeSets.append(format(
                    BATCH_UPDATE_CHANGE_SET_BODY_FOR_UPDATING_VALUE_TEMPLATE,
                    URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    URLEncoder.encode(valueForUpdating.getKey(), StandardCharsets.UTF_8.name()).replace("+", "%20"),
                    requestId,
                    valueForUpdating.getValue().length() + 12,
                    valueForUpdating.getValue()
            ));
        }
    }

    private static void addChangeSetsForAddedValues(
            StringBuilder changeSets,
            String keyMapEntry,
            Map<String, String> valuesForAdding,
            String changeSetSeparator,
            String requestId
    ) throws UnsupportedEncodingException {
        for (Map.Entry<String, String> valueForAdding : valuesForAdding.entrySet()) {
            changeSets.append(format(BATCH_UPDATE_CHANGE_SET_START_TEMPLATE, changeSetSeparator));

            String payload = format(
                    PAYLOAD_FOR_ADDING_VALUE_TEMPLATE,
                    valueForAdding.getKey(),
                    valueForAdding.getValue(),
                    keyMapEntry,
                    URLEncoder.encode(keyMapEntry, StandardCharsets.UTF_8.name()).replace("+", "%20")
            );

            changeSets.append(format(
                    BATCH_UPDATE_CHANGE_SET_BODY_FOR_ADDING_VALUE_TEMPLATE,
                    requestId,
                    payload.length(),
                    payload
            ));
        }
    }
}
