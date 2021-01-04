package com.figaf.integration.apimgmt.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;
import java.util.List;

@Getter
@Setter
@ToString
public class KeyMapEntryMetaData {

    private String name;
    private String scope;
    private boolean encrypted;

    @JsonIgnore
    private Date creationDate;
    @JsonIgnore
    private String createdBy;
    @JsonIgnore
    private Date modificationDate;
    @JsonIgnore
    private String modifiedBy;

    //for creating  new entry
    List<KeyMapEntryValue> keyMapEntryValues;

}
