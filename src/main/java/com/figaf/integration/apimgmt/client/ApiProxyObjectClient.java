package com.figaf.integration.apimgmt.client;

import com.figaf.integration.apimgmt.entity.ApiProxyMetaData;
import com.figaf.integration.apimgmt.response_parser.ApiProxyObjectParser;
import com.figaf.integration.common.client.BaseClient;
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

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public List<ApiProxyMetaData> getPublicApiObjectMetaData(RequestContext requestContext) {
        log.debug("#getApiObjectMetaData(RequestContext requestContext): {}", requestContext);
        return executeGetPublicApiAndReturnResponseBody(requestContext, API_PROXIES, ApiProxyObjectParser::buildApiProxyMetaDataList);
    }

    public List<ApiProxyMetaData> getApiObjectMetaData(RequestContext requestContext) {
        log.debug("#getApiObjectMetaData(RequestContext requestContext): {}", requestContext);
        return executeGet(requestContext, API_PROXIES, ApiProxyObjectParser::buildApiProxyMetaDataList);
    }

    public ApiProxyMetaData getApiObjectMetaData(RequestContext requestContext, String apiProxyName) {
        log.debug("#getApiObjectMetaData(RequestContext requestContext, String apiProxyName): {}, {}", requestContext, apiProxyName);
        ApiProxyMetaData apiProxyMetaData = null;
        try {
            apiProxyMetaData = executeGet(
                requestContext,
                format(API_PROXY_WITH_INNER_OBJECTS_METADATA, apiProxyName),
                ApiProxyObjectParser::buildApiProxyMetaData
            );
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

        return executeGet(
                requestContext,
                path,
                body -> ApiProxyObjectParser.buildInnerObjectsNameToApiProxyMetaDataMap(body, innerObjectNames)
        );

    }

    public byte[] downloadApiProxy(RequestContext requestContext, String apiProxyName) {
        log.debug("#downloadApiProxy(RequestContext requestContext, String apiProxyName): {}, {}", requestContext, apiProxyName);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Accept", "application/json");
        return executeGetPublicApiAndReturnResponseBody(
                requestContext,
                String.format(API_PROXIES_TRANSPORT_WITH_NAME, apiProxyName),
                httpHeaders,
                resolvedBody -> resolvedBody,
                byte[].class
        );
    }

    public <R> R downloadApiProxyPublicApi(
            RequestContext requestContext,
            String apiProxyName,
            ResponseHandlerCallback<R, ResponseEntity<String>> responseHandlerCallback
    ) {
        try {
            return executeMethodPublicApi(
                    requestContext,
                    String.format(API_PROXIES_TRANSPORT_WITH_NAME, apiProxyName),
                    null,
                    HttpMethod.GET,
                    responseHandlerCallback
            );
        } catch (HttpClientErrorException.NotFound notFoundException) {
            log.debug("Can't downloadApiProxyPublicApi (NotFound error): {}", ExceptionUtils.getMessage(notFoundException));
            try {
                return responseHandlerCallback.apply(null);
            } catch (Exception ex) {
                log.error("Can't apply responseHandlerCallback: ", ex);
                throw new ClientIntegrationException(ex);
            }
        } catch (Exception ex) {
            log.error("Can't downloadApiProxyPublicApi: ", ex);
            throw new ClientIntegrationException(ex);
        }
    }


    public void uploadApiProxy(RequestContext requestContext, String apiProxyName, byte[] bundledApiProxy) {
        log.debug("#uploadApiProxy(RequestContext requestContext, String apiProxyName, byte[] bundledApiProxy): {}, {}", requestContext, apiProxyName);

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

    public void deleteApiProxy(String apiProxyId, RequestContext requestContext) {
        log.debug("#deleteApiProxy(String apiProxyId, RequestContext requestContext): {}, {}", apiProxyId, requestContext);
        executeMethod(
            requestContext,
            API_PROXIES,
            format(API_PROXIES_WITH_NAME, apiProxyId),
            (url, token, restTemplateWrapper) -> {
                deleteApiProxy(apiProxyId, url, token, restTemplateWrapper.getRestTemplate());
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
