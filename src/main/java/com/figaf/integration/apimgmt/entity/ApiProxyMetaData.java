package com.figaf.integration.apimgmt.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Getter
@Setter
@ToString
public class ApiProxyMetaData {

    private String name;
    private String title;
    private String version;
    private String state;
    private String apiType;
    private Date creationDate;
    private String createdBy;
    private Date modificationDate;
    private String modifiedBy;
    private boolean isChanged;
    private boolean isVersioned;

}
