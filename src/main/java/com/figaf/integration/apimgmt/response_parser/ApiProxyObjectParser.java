package com.figaf.integration.apimgmt.response_parser;

import com.figaf.integration.apimgmt.entity.ApiProxyMetaData;
import com.figaf.integration.common.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author Arsenii Istlentev
 */
public class ApiProxyObjectParser {

    public static List<ApiProxyMetaData> buildApiProxyMetaDataList(String body) {
        JSONObject response = new JSONObject(body);
        JSONArray apiProxyJsonArray = response.getJSONObject("d").getJSONArray("results");

        List<ApiProxyMetaData> apiProxyMetaDataList = new ArrayList<>();
        for (int ind = 0; ind < apiProxyJsonArray.length(); ind++) {
            JSONObject apiProxyElement = apiProxyJsonArray.getJSONObject(ind);
            apiProxyMetaDataList.add(parseApiProxyResponse(apiProxyElement));
        }
        return apiProxyMetaDataList;
    }

    public static ApiProxyMetaData buildApiProxyMetaData(String body) {
        JSONObject response = new JSONObject(body);
        JSONObject jsonObject = response.getJSONObject("d");
        return parseApiProxyResponse(jsonObject);
    }


    public static Map<String, ApiProxyMetaData> buildInnerObjectsNameToApiProxyMetaDataMap(String body, Set<String> innerObjectNames) {
        JSONObject response = new JSONObject(body);
        JSONObject apiProxyObject = response.getJSONObject("d");

        Map<String, ApiProxyMetaData> innerObjectsNameToApiProxyMetaDataMap = new HashMap<>();
        for (String innerObjectsName : innerObjectNames) {
            JSONObject innerObject = apiProxyObject.getJSONObject(innerObjectsName);

            if (innerObject == null) {
                continue;
            }

            JSONArray innerObjectsJsonArray = innerObject.getJSONArray("results");

            if (innerObjectsJsonArray != null) {

                for (int ind = 0; ind < innerObjectsJsonArray.length(); ind++) {
                    JSONObject innerObjectElement = innerObjectsJsonArray.getJSONObject(ind);

                    ApiProxyMetaData apiProxyMetaData = new ApiProxyMetaData();
                    String name = Utils.optString(innerObjectElement, "name");
                    apiProxyMetaData.setName(name);

                    if (name == null) {
                        continue;
                    }

                    JSONObject innerObjectLifeCycleElement = innerObjectElement.optJSONObject("life_cycle");
                    if (innerObjectLifeCycleElement == null) {
                        continue;
                    }

                    String createdDateStr = Utils.optString(innerObjectLifeCycleElement, "created_at");
                    apiProxyMetaData.setCreationDate(StringUtils.isNotBlank(createdDateStr)
                        ? new Timestamp(Long.parseLong(createdDateStr.replaceAll("[^0-9]", "")))
                        : null
                    );
                    String createdBy = Utils.optString(innerObjectLifeCycleElement, "created_by");
                    apiProxyMetaData.setCreatedBy(createdBy);
                    String changedDateStr = Utils.optString(innerObjectLifeCycleElement, "changed_at");
                    apiProxyMetaData.setModificationDate(StringUtils.isNotBlank(changedDateStr)
                        ? new Timestamp(Long.parseLong(changedDateStr.replaceAll("[^0-9]", "")))
                        : null
                    );
                    String changedBy = Utils.optString(innerObjectLifeCycleElement, "changed_by");
                    apiProxyMetaData.setModifiedBy(changedBy);

                    innerObjectsNameToApiProxyMetaDataMap.put(String.format("%s|%s", innerObjectsName, name), apiProxyMetaData);
                }
            }
        }

        return innerObjectsNameToApiProxyMetaDataMap;
    }

    private static ApiProxyMetaData parseApiProxyResponse(JSONObject apiProxyJsonObject) throws JSONException {
        ApiProxyMetaData apiProxyMetaData = new ApiProxyMetaData();
        apiProxyMetaData.setName(apiProxyJsonObject.getString("name"));
        apiProxyMetaData.setTitle(apiProxyJsonObject.getString("title"));
        apiProxyMetaData.setVersion(apiProxyJsonObject.getString("version"));
        apiProxyMetaData.setState(apiProxyJsonObject.getString("state"));
        apiProxyMetaData.setApiType(apiProxyJsonObject.getString("service_code"));
        String isVersioned = Utils.optString(apiProxyJsonObject, "isVersioned");
        apiProxyMetaData.setVersioned(StringUtils.isNotBlank(isVersioned) && Boolean.parseBoolean(isVersioned));

        JSONObject apiProxyLifeCycleElement = apiProxyJsonObject.getJSONObject("life_cycle");
        String createdAt = Utils.optString(apiProxyLifeCycleElement, "created_at");
        apiProxyMetaData.setCreationDate(createdAt != null
            ? new Timestamp(Long.parseLong(createdAt.replaceAll("[^0-9]", "")))
            : null
        );
        apiProxyMetaData.setCreatedBy(Utils.optString(apiProxyLifeCycleElement, "created_by"));
        String changedAt = Utils.optString(apiProxyLifeCycleElement, "changed_at");
        apiProxyMetaData.setModificationDate(changedAt != null
            ? new Timestamp(Long.parseLong(changedAt.replaceAll("[^0-9]", "")))
            : null
        );
        apiProxyMetaData.setModifiedBy(Utils.optString(apiProxyLifeCycleElement, "changed_by"));

        return apiProxyMetaData;
    }
}
