package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.entity.ApiProxyMetaData;
import com.figaf.integration.apimgmt.response_parser.ApiProxyObjectParser;
import com.figaf.integration.common.client.wrapper.CommonClientWrapper;
import com.figaf.integration.common.entity.CommonClientWrapperEntity;
import com.figaf.integration.common.exception.ClientIntegrationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Arsenii Istlentev
 */

@Slf4j
public class ApiProxyObjectClient extends CommonClientWrapper {

    private static final String API_PROXIES = "/apiportal/api/1.0/Management.svc/APIProxies?$format=json";
    private static final String API_PROXY_WITH_INNER_OBJECTS_METADATA = "/apiportal/api/1.0/Management.svc/APIProxies('%s')?$format=json";
    private static final String API_PROXIES_TRANSPORT_WITH_NAME = "/apiportal/api/1.0/Transport.svc/APIProxies?name=%s";
    private static final String API_PROXIES_TRANSPORT = "/apiportal/api/1.0/Transport.svc/APIProxies";

    public ApiProxyObjectClient(String ssoUrl) {
        super(ssoUrl);
    }

    public List<ApiProxyMetaData> getApiObjectMetaData(CommonClientWrapperEntity commonClientWrapperEntity) {
        log.debug("#getApiObjectMetaData(CommonClientWrapperEntity commonClientWrapperEntity): {}", commonClientWrapperEntity);
        return executeGet(commonClientWrapperEntity, API_PROXIES, ApiProxyObjectParser::buildApiProxyMetaDataList);
    }

    public ApiProxyMetaData getApiObjectMetaData(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName) {
        log.debug("#getApiObjectMetaData(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName): {}, {}", commonClientWrapperEntity, apiProxyName);
        return executeGet(commonClientWrapperEntity, String.format(API_PROXY_WITH_INNER_OBJECTS_METADATA, apiProxyName), ApiProxyObjectParser::buildApiProxyMetaData);
    }

    public Map<String, ApiProxyMetaData> getApiObjectMetaDataForInnerObjects(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName, Set<String> innerObjectNames) {
        log.debug("#getApiObjectMetaDataWithInnerdObjects(CommonClientWrapperEntity commonClientWrapperEntity, Set<String> innerObjectNames): {}, {}", commonClientWrapperEntity, innerObjectNames);
        String path = String.format(API_PROXY_WITH_INNER_OBJECTS_METADATA, apiProxyName);
        if (CollectionUtils.isNotEmpty(innerObjectNames)) {
            path = String.format("%s&$expand=%s", path, StringUtils.join(innerObjectNames, ","));
        }

        return executeGet(
                commonClientWrapperEntity,
                path,
                body -> ApiProxyObjectParser.buildInnerObjectsNameToApiProxyMetaDataMap(body, innerObjectNames)
        );

    }

    public byte[] downloadApiProxy(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName) {
        log.debug("#downloadApiProxy(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName): {}, {}", commonClientWrapperEntity, apiProxyName);
        return executeGet(
                commonClientWrapperEntity,
                String.format(API_PROXIES_TRANSPORT_WITH_NAME, apiProxyName),
                resolvedBody -> resolvedBody,
                byte[].class
        );
    }

    public void uploadApiProxy(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName, byte[] bundledApiProxy) {
        log.debug("#uploadApiProxy(CommonClientWrapperEntity commonClientWrapperEntity, String apiProxyName, byte[] bundledApiProxy): {}, {}", commonClientWrapperEntity, apiProxyName);

        executeMethod(
                commonClientWrapperEntity,
                "/apiportal/api/1.0/Management.svc/APIProxies",
                API_PROXIES_TRANSPORT,
                (url, token, restTemplateWrapper) -> {
                    uploadApiProxy(bundledApiProxy, url, token, restTemplateWrapper.getRestTemplate());
                    return null;
                }
        );
    }

    private void uploadApiProxy(byte[] bundledApiProxy, String url, String token, RestTemplate restTemplate) {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("X-CSRF-Token", token);
        httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        HttpEntity<byte[]> requestEntity = new HttpEntity<>(Base64.getEncoder().encode(bundledApiProxy), httpHeaders);

        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);

        if (!HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException("Couldn't execute api proxy uploading:\n" +
                    responseEntity.getBody()
            );
        }

    }

}
