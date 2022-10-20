package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.entity.ApiProxyMetaData;
import com.figaf.integration.apimgmt.response_parser.ApiProxyObjectParser;
import com.figaf.integration.apimgmt.response_parser.KeyMapEntriesParser;
import com.figaf.integration.common.client.BaseClient;
import com.figaf.integration.common.entity.AuthenticationType;
import com.figaf.integration.common.entity.RequestContext;
import com.figaf.integration.common.exception.ClientIntegrationException;
import com.figaf.integration.common.factory.HttpClientsFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.figaf.integration.common.entity.AuthenticationType.OAUTH;
import static java.lang.String.format;
import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * @author Arsenii Istlentev
 */

@Slf4j
public class ApiProxyObjectClient extends BaseClient {

    private static final String API_PROXIES = "/apiportal/api/1.0/Management.svc/APIProxies?$format=json";
    private static final String API_PROXIES_WITH_NAME = "/apiportal/api/1.0/Management.svc/APIProxies('%s')";
    private static final String API_PROXY_WITH_INNER_OBJECTS_METADATA = "/apiportal/api/1.0/Management.svc/APIProxies('%s')?$format=json";
    private static final String API_PROXIES_TRANSPORT_WITH_NAME = "/apiportal/api/1.0/Transport.svc/APIProxies?name=%s";
    private static final String API_PROXIES_TRANSPORT = "/apiportal/api/1.0/Transport.svc/APIProxies";

    public ApiProxyObjectClient(HttpClientsFactory httpClientsFactory) {
        super(httpClientsFactory);
    }

    public List<ApiProxyMetaData> getApiObjectMetaData(RequestContext requestContext) {
        log.debug("#getApiObjectMetaData(RequestContext requestContext): {}", requestContext);
        AuthenticationType authenticationType = requestContext.getAuthenticationType();
        if (OAUTH.equals(authenticationType)) {
            return executeGetPublicApiAndReturnResponseBody(
                    requestContext,
                    API_PROXIES,
                    ApiProxyObjectParser::buildApiProxyMetaDataList
            );
        }
        return executeGet(
                requestContext,
                API_PROXIES,
                ApiProxyObjectParser::buildApiProxyMetaDataList
        );
    }

    public ApiProxyMetaData getApiObjectMetaData(RequestContext requestContext, String apiProxyName) {
        log.debug("#getApiObjectMetaData(RequestContext requestContext, String apiProxyName): {}, {}", requestContext, apiProxyName);
        ApiProxyMetaData apiProxyMetaData = null;
        try {
            if (OAUTH.equals(requestContext.getAuthenticationType())) {
                apiProxyMetaData = executeGetPublicApiAndReturnResponseBody(
                        requestContext,
                        format(API_PROXY_WITH_INNER_OBJECTS_METADATA, apiProxyName),
                        ApiProxyObjectParser::buildApiProxyMetaData
                );
            } else {
                apiProxyMetaData = executeGet(
                        requestContext,
                        format(API_PROXY_WITH_INNER_OBJECTS_METADATA, apiProxyName),
                        ApiProxyObjectParser::buildApiProxyMetaData
                );
            }
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
                !NOT_FOUND.equals(((HttpStatusCodeException)ex.getCause()).getStatusCode())
            ) {
                throw ex;
            }
        }
        return apiProxyMetaData;
    }

    public Map<String, ApiProxyMetaData> getApiObjectMetaDataForInnerObjects(RequestContext requestContext, String apiProxyName, Set<String> innerObjectNames) {
        log.debug("#getApiObjectMetaDataWithInnerdObjects(RequestContext requestContext, Set<String> innerObjectNames): {}, {}", requestContext, innerObjectNames);
        String path = String.format(API_PROXY_WITH_INNER_OBJECTS_METADATA, apiProxyName);
        if (CollectionUtils.isNotEmpty(innerObjectNames)) {
            path = String.format("%s&$expand=%s", path, StringUtils.join(innerObjectNames, ","));
        }
        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            return executeMethodPublicApi(
                    requestContext,
                    path,
                    null,
                    HttpMethod.GET,
                    response -> ApiProxyObjectParser.buildInnerObjectsNameToApiProxyMetaDataMap(response.getBody(), innerObjectNames)
            );
        }
        return executeGet(
                requestContext,
                path,
                body -> ApiProxyObjectParser.buildInnerObjectsNameToApiProxyMetaDataMap(body, innerObjectNames)
        );
    }

    public byte[] downloadApiProxy(RequestContext requestContext, String apiProxyName) {
        log.debug("#downloadApiProxy(RequestContext requestContext, String apiProxyName): {}, {}", requestContext, apiProxyName);
        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            return executeMethodPublicApi(
                    requestContext,
                    String.format(API_PROXIES_TRANSPORT_WITH_NAME, apiProxyName),
                    null,
                    HttpMethod.GET,
                    HttpEntity::getBody,
                    byte[].class
            );
        }
        return executeGet(
                requestContext,
                String.format(API_PROXIES_TRANSPORT_WITH_NAME, apiProxyName),
                resolvedBody -> resolvedBody,
                byte[].class
        );
    }

    public void uploadApiProxy(RequestContext requestContext, String apiProxyName, byte[] bundledApiProxy) {
        log.debug("#uploadApiProxy(RequestContext requestContext, String apiProxyName, byte[] bundledApiProxy): {}, {}", requestContext, apiProxyName);
        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            byte[] bundledApiProxyEncoded = Base64.getEncoder().encode(bundledApiProxy);

            executeMethodPublicApiWithCustomHeaders(
                    requestContext,
                    API_PROXIES_TRANSPORT,
                    bundledApiProxyEncoded,
                    HttpMethod.POST,
                    httpHeaders,
                    response -> {
                        if (!HttpStatus.OK.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException("Couldn't execute api proxy uploading:\n" +
                                    response.getBody()
                            );
                        }
                        return null;
                    }
            );
        } else {
            executeMethod(
                    requestContext,
                    API_PROXIES,
                    API_PROXIES_TRANSPORT,
                    (url, token, restTemplateWrapper) -> {
                        uploadApiProxy(bundledApiProxy, url, token, restTemplateWrapper.getRestTemplate());
                        return null;
                    }
            );
        }
    }

    public void deleteApiProxy(String apiProxyId, RequestContext requestContext) {
        log.debug("#deleteApiProxy(String apiProxyId, RequestContext requestContext): {}, {}", apiProxyId, requestContext);
        String pathForMainRequest = format(API_PROXIES_WITH_NAME, apiProxyId);

        if (OAUTH.equals(requestContext.getAuthenticationType())) {
            executeDeletePublicApi(
                    requestContext,
                    pathForMainRequest,
                    response -> {
                        if (!HttpStatus.NO_CONTENT.equals(response.getStatusCode())) {
                            throw new ClientIntegrationException(format(
                                    "Couldn't delete api proxy %s: Code: %d, Message: %s",
                                    apiProxyId,
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
                    API_PROXIES,
                    pathForMainRequest,
                    (url, token, restTemplateWrapper) -> {
                        deleteApiProxy(apiProxyId, url, token, restTemplateWrapper.getRestTemplate());
                        return null;
                    }
            );
        }
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

    private void deleteApiProxy(String apiProxyId, String url, String token, RestTemplate restTemplate) {

        HttpHeaders httpHeaders = createHttpHeadersWithCSRFToken(token);
        HttpEntity<Void> httpEntity = new HttpEntity<>(httpHeaders);
        ResponseEntity<String> responseEntity = restTemplate.exchange(url, DELETE, httpEntity, String.class);
        if (!HttpStatus.NO_CONTENT.equals(responseEntity.getStatusCode())) {
            throw new ClientIntegrationException(format(
                "Couldn't delete api proxy %s: Code: %d, Message: %s",
                apiProxyId,
                responseEntity.getStatusCode().value(),
                responseEntity.getBody())
            );
        }

    }

}
