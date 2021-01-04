package com.figaf.integration.apimgmt.response_parser;

import com.figaf.integration.apimgmt.entity.KeyMapEntryMetaData;
import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.common.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arsenii Istlentev
 */
@Slf4j
public class KeyMapEntriesParser {

    public static List<String> buildKeyMapEntryList(String body) {
        JSONObject response = new JSONObject(body);
        JSONArray keyMapEntriesJsonArray = response.getJSONObject("d").getJSONArray("results");

        List<String> keyMapEntries = new ArrayList<>();
        for (int ind = 0; ind < keyMapEntriesJsonArray.length(); ind++) {
            JSONObject keyMapEntryElement = keyMapEntriesJsonArray.getJSONObject(ind);
            keyMapEntries.add(Utils.optString(keyMapEntryElement, "name"));
        }

        return keyMapEntries;
    }

    public static KeyMapEntryMetaData buildKeyMapEntryMetaData(String body) {
        JSONObject response = new JSONObject(body);
        JSONObject keyMapEntryJsonObject = response.getJSONObject("d");
        return parseKeyMapEntryMetaData(keyMapEntryJsonObject);
    }

    public static List<KeyMapEntryMetaData> buildKeyMapEntryMetaDataList(String body) {
        JSONObject response = new JSONObject(body);
        JSONArray keyMapEntryJsonArray = response.getJSONObject("d").getJSONArray("results");

        List<KeyMapEntryMetaData> keyMapEntries = new ArrayList<>();
        for (int ind = 0; ind < keyMapEntryJsonArray.length(); ind++) {
            keyMapEntries.add(parseKeyMapEntryMetaData(keyMapEntryJsonArray.getJSONObject(ind)));
        }

        return keyMapEntries;
    }

    public static List<KeyMapEntryValue> buildKeyMapEntryValuesList(String keyMapEntry, String body) {
        JSONObject response = new JSONObject(body);
        JSONArray keyMapEntryValuesJsonArray = response.getJSONObject("d").getJSONArray("results");

        List<KeyMapEntryValue> keyMapEntryValues = new ArrayList<>();
        for (int ind = 0; ind < keyMapEntryValuesJsonArray.length(); ind++) {
            JSONObject keyMapEntryValueElement = keyMapEntryValuesJsonArray.getJSONObject(ind);

            String keyMapEntryValueName = Utils.optString(keyMapEntryValueElement, "name");

            String keyMapEntryValue = Utils.optString(keyMapEntryValueElement, "value");

            keyMapEntryValues.add(new KeyMapEntryValue(keyMapEntry, keyMapEntryValueName, keyMapEntryValue));
        }

        return keyMapEntryValues;
    }

    private static KeyMapEntryMetaData parseKeyMapEntryMetaData(JSONObject keyMapEntryElement) {
        KeyMapEntryMetaData keyMapEntry = new KeyMapEntryMetaData();
        keyMapEntry.setName(keyMapEntryElement.getString("name"));
        keyMapEntry.setScope(keyMapEntryElement.getString("scope"));
        keyMapEntry.setEncrypted(keyMapEntryElement.getBoolean("encrypted"));

        JSONObject keyMapEntryLifeCycleElement = keyMapEntryElement.getJSONObject("life_cycle");
        String createdAt = Utils.optString(keyMapEntryLifeCycleElement, "created_at");
        keyMapEntry.setCreationDate(createdAt != null
                ? new Timestamp(Long.parseLong(createdAt.replaceAll("[^0-9]", "")))
                : null
        );
        keyMapEntry.setCreatedBy(Utils.optString(keyMapEntryLifeCycleElement, "created_by"));
        String changedAt = Utils.optString(keyMapEntryLifeCycleElement, "changed_at");
        keyMapEntry.setModificationDate(changedAt != null
                ? new Timestamp(Long.parseLong(changedAt.replaceAll("[^0-9]", "")))
                : null
        );
        keyMapEntry.setModifiedBy(Utils.optString(keyMapEntryLifeCycleElement, "changed_by"));
        return keyMapEntry;
    }

}
