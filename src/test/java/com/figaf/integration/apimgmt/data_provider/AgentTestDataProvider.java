package com.figaf.integration.apimgmt.data_provider;

import com.figaf.integration.common.data_provider.AbstractAgentTestDataProvider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * @author Ilya Nesterov
 */
public class AgentTestDataProvider extends AbstractAgentTestDataProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
                Arguments.of(buildAgentTestData(Paths.get("src/test/resources/agent-test-data/apimgmt-cf-basic"))),
                Arguments.of(buildAgentTestData(Paths.get("src/test/resources/agent-test-data/apimgmt-cf-oauth")))
        );
    }
}
