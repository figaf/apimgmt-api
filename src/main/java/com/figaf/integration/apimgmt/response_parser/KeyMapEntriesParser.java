package com.figaf.integration.apimgmt.response_parser;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.figaf.integration.apimgmt.entity.KeyMapEntryValue;
import com.figaf.integration.common.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author Arsenii Istlentev
 */
@Slf4j
public class KeyMapEntriesParser {

    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    static {
        JSON_OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JSON_OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        JSON_OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("E, d MMM yyyy HH:mm:ss z", Locale.ENGLISH));
    }

    public static List<String> buidKeyMapEntryList(String body) {
        JSONObject response = new JSONObject(body);
        JSONArray keyMapEntriesJsonArray = response.getJSONObject("d").getJSONArray("results");

        List<String> keyMapEntries = new ArrayList<>();
        for (int ind = 0; ind < keyMapEntriesJsonArray.length(); ind++) {
            JSONObject keyMapEntryElement = keyMapEntriesJsonArray.getJSONObject(ind);
            keyMapEntries.add(Utils.optString(keyMapEntryElement, "name"));
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

            String valueJsonObjectString = Utils.optString(keyMapEntryValueElement, "value");

            if (valueJsonObjectString == null) {
                log.debug(String.format("KeyMapEntryValue element %s of %s map has null value, skipping its processing.",
                    Utils.optString(keyMapEntryValueElement, "name"),
                    keyMapEntry
                ));
                continue;
            }

            try {
                KeyMapEntryValue.Value valueObject = JSON_OBJECT_MAPPER.readValue(valueJsonObjectString, KeyMapEntryValue.Value.class);
                keyMapEntryValues.add(new KeyMapEntryValue(
                    keyMapEntry,
                    keyMapEntryValueName,
                    valueObject
                ));
            } catch (Exception ex) {
                log.warn(String.format("Couldn't parse value of kvm entry <%s,%s>: %s. Error: %s",
                    keyMapEntry,
                    keyMapEntryValueName,
                    valueJsonObjectString,
                    ExceptionUtils.getRootCause(ex)
                ));
            }
        }

        return keyMapEntryValues;
    }

}
