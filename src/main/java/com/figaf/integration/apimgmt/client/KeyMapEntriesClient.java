package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.apimgmt.response_parser.KeyMapEntriesParser;
import com.figaf.integration.common.client.BaseClient;
import com.figaf.integration.common.entity.RequestContext;
import com.figaf.integration.common.exception.ClientIntegrationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * @author Arsenii Istlentev
 */
@Slf4j
public class KeyMapEntriesClient extends BaseClient {

    private static final String KEY_MAP_ENTRIES = "/apiportal/api/1.0/Management.svc/KeyMapEntries?forceUpdateFromRT=true&$format=json";
    private static final String KEY_MAP_ENTRY_VALUES = "/apiportal/api/1.0/Management.svc/KeyMapEntries('%s')/keyMapEntryValues?forceUpdateFromRT=true&$format=json";
    private static final String KEY_MAP_ENTRY_VALUE = "/apiportal/api/1.0/Management.svc/KeyMapEntryValues(map_name='%s',name='%s')";

    public KeyMapEntriesClient(String ssoUrl) {
        super(ssoUrl);
    }

    public List<String> getKeyMapEntries(RequestContext requestContext) {
        log.debug("#getKeyMapEntries(RequestContext requestContext): {}", requestContext);
        return executeGet(requestContext, KEY_MAP_ENTRIES, KeyMapEntriesParser::buidKeyMapEntryList);
    }

    public List<KeyMapEntryValue> getKeyMapEntryValues(String keyMapEntry, RequestContext requestContext) {
        log.debug("#getKeyMapEntryValues(String keyMapEntry, RequestContext requestContext): {}, {}", keyMapEntry, requestContext);
        return executeGet(
                requestContext,
                String.format(KEY_MAP_ENTRY_VALUES, keyMapEntry),
                body -> KeyMapEntriesParser.buildKeyMapEntryValuesList(keyMapEntry, body)
        );
    }

    public void deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, RequestContext requestContext) {
        log.debug("#deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, RequestContext requestContext): {}, {}, {}", keyMapEntry, keyMapEntryValueName, requestContext);

        executeMethod(
                requestContext,
                "/apiportal/api/1.0/Management.svc/KeyMapEntryValues",
                String.format(KEY_MAP_ENTRY_VALUE, keyMapEntry, keyMapEntryValueName),
                (url, token, restTemplateWrapper) -> {
                    deleteKeyMapEntryValue(keyMapEntry, keyMapEntryValueName, url, token, restTemplateWrapper.getRestTemplate());
                    return null;
                }
        );

    }

    private void deleteKeyMapEntryValue(String keyMapEntry, String keyMapEntryValueName, String url, String token, RestTemplate restTemplate) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        HttpEntity<Void> httpEntity = new HttpEntity<>(httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.DELETE, httpEntity, String.class);

        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(String.format(
                    "Couldn't delete key map entry value <%s, %s>: Code: %d, Message: %s",
                    keyMapEntry,
                    keyMapEntryValueName,
                    responseEntity.getStatusCode().value(),
                    responseEntity.getBody())
            );
        }
    }

}
