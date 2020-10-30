package com.figaf.integration.apimgmt.entity;

import lombok.*;

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

    private String mapName;
    private String name;
    private Value value;

    @Getter
    @Setter
    @ToString
    public static class Value implements Serializable {

        private String messageId;
        private Date currentSystemTime;
        private Date clientReceivedStartTime;
        private int timePassedAfterClientReceivedStartTime;
        private String messageQueryString;
        private String requestUri;
        private String apiProxyName;
        private String apiProxyRevision;
        private String faultName;
        private String errorContent;
        private String errorMessage;
        private int errorStatusCode;
    }
}
