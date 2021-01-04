package com.figaf.integration.apimgmt.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.json.JSONPropertyName;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Ilya Nesterov
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class KeyMapEntryValue implements Serializable {

    @JsonProperty("map_name")
    private String mapName;
    private String name;
    private String value;

}
