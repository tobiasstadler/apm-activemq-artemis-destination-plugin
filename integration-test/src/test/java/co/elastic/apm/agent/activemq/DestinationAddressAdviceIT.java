/*
   Copyright 2021 Tobias Stadler

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package co.elastic.apm.agent.activemq;

import co.elastic.apm.api.ElasticApm;
import co.elastic.apm.api.Scope;
import co.elastic.apm.api.Span;
import co.elastic.apm.api.Transaction;
import co.elastic.apm.attach.ElasticApmAttacher;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.ClearType;
import org.mockserver.model.Format;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

@ExtendWith(MockServerExtension.class)
class DestinationAddressAdviceIT {

    private static ActiveMQServerImpl ACTIVEMQ_SERVER;

    private static MockServerClient MOCK_SERVER_CLIENT;

    @BeforeAll
    static void setUp(MockServerClient mockServerClient) throws Exception {
        MOCK_SERVER_CLIENT = mockServerClient;
        MOCK_SERVER_CLIENT.when(request("/")).respond(response().withStatusCode(200).withBody(json("{\"version\": \"7.13.0\"}")));
        MOCK_SERVER_CLIENT.when(request("/config/v1/agents")).respond(response().withStatusCode(403));
        MOCK_SERVER_CLIENT.when(request("/intake/v2/events")).respond(response().withStatusCode(200));

        HashSet<TransportConfiguration> transports = new HashSet<>();
        transports.add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
        transports.add(new TransportConfiguration(NettyAcceptorFactory.class.getName(), Collections.singletonMap("port", 61616)));

        ACTIVEMQ_SERVER = new ActiveMQServerImpl(new ConfigurationImpl()
                .setAcceptorConfigurations(transports)
                .setSecurityEnabled(false)
                .setPersistenceEnabled(false));
        ACTIVEMQ_SERVER.start();

        Map<String, String> configuration = new HashMap<>();
        configuration.put("server_url", "http://localhost:" + mockServerClient.getPort());
        configuration.put("report_sync", "true");
        configuration.put("disable_metrics", "*");
        configuration.put("plugins_dir", "target/apm-plugins");
        configuration.put("application_packages", "co.elastic.apm.agent.activemq");

        ElasticApmAttacher.attach(configuration);
    }

    @AfterAll
    static void tearDown() throws Exception {
        ACTIVEMQ_SERVER.stop();
    }

    @BeforeEach
    void clear() {
        MOCK_SERVER_CLIENT.clear(request("/intake/v2/events"), ClearType.LOG);
    }

    @Test
    public void testNettyConnection() {
        Transaction transaction = ElasticApm.startTransaction();
        try (Scope scope = transaction.activate()) {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            try (JMSContext context = connectionFactory.createContext()) {
                context.createProducer().send(context.createQueue("netty"), context.createMessage());
            }
        } finally {
            transaction.end();
        }

        Map<String, Object> clientSpan = getClientSpan();

        assertEquals("127.0.0.1", JsonPath.read(clientSpan, "$.context.destination.address"));
        assertEquals(61616, (int) JsonPath.read(clientSpan, "$.context.destination.port"));
    }

    @Test
    public void testInVMConnection() {
        Transaction transaction = ElasticApm.startTransaction();
        try (Scope scope = transaction.activate()) {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://0");
            try (JMSContext context = connectionFactory.createContext()) {
                context.createProducer().send(context.createQueue("vm"), context.createMessage());
            }
        } finally {
            transaction.end();
        }

        Map<String, Object> clientSpan = getClientSpan();

        assertThrows(PathNotFoundException.class, () -> JsonPath.read(clientSpan, "$.context.destination.address"));
        assertThrows(PathNotFoundException.class, () -> JsonPath.read(clientSpan, "$.context.destination.port"));
    }

    @Test
    public void testNonExitSpan() throws Exception {
        Transaction transaction = ElasticApm.startTransaction();

        Span nonExitSpan = transaction.startSpan("messaging", null, null);
        try (Scope scope = nonExitSpan.activate()) {
            ClientSession session = ActiveMQClient.createServerLocator("tcp://localhost:61616")
                    .createSessionFactory()
                    .createSession();

            session.createProducer("no-exit-span").send(session.createMessage(false));
        } finally {
            nonExitSpan.end();
            transaction.end();
        }

        Map<String, Object> clientSpan = getClientSpan();

        assertThrows(PathNotFoundException.class, () -> JsonPath.read(clientSpan, "$.context.destination.address"));
        assertThrows(PathNotFoundException.class, () -> JsonPath.read(clientSpan, "$.context.destination.port"));
    }

    @Test
    public void testNonMessagingSpan() throws Exception {
        Transaction transaction = ElasticApm.startTransaction();

        Span nonMessagingSpan = transaction.startExitSpan("foo", null, null);
        try (Scope scope = nonMessagingSpan.activate()) {
            ClientSession session = ActiveMQClient.createServerLocator("tcp://localhost:61616")
                    .createSessionFactory()
                    .createSession();

            session.createProducer("no-messaging-span").send(session.createMessage(false));
        } finally {
            nonMessagingSpan.end();
            transaction.end();
        }

        Map<String, Object> clientSpan = getClientSpan();

        assertThrows(PathNotFoundException.class, () -> JsonPath.read(clientSpan, "$.context.destination.address"));
        assertThrows(PathNotFoundException.class, () -> JsonPath.read(clientSpan, "$.context.destination.port"));
    }

    private static Map<String, Object> getClientSpan() {
        return getEvents()
                .flatMap(dc -> ((List<Map<String, Object>>) dc.read("$[?(@.span)].span")).stream())
                .findAny()
                .get();
    }

    private static Stream<DocumentContext> getEvents() {
        return ((List<String>) JsonPath.read(MOCK_SERVER_CLIENT.retrieveRecordedRequests(request("/intake/v2/events"), Format.JAVA), "$..body.rawBytes"))
                .stream()
                .map(Base64.getDecoder()::decode)
                .map(String::new)
                .flatMap(s -> Arrays.stream(s.split("\r?\n")))
                .map(JsonPath::parse);
    }
}