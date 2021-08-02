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

import co.elastic.apm.api.AbstractSpanImplAccessor;
import co.elastic.apm.api.ElasticApm;
import co.elastic.apm.api.Span;
import net.bytebuddy.asm.Advice;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public class DestinationAddressAdvice {

    private static final MethodHandle isExit = getMethodHandle("co.elastic.apm.agent.impl.transaction.Span", "isExit", boolean.class);

    private static final MethodHandle getType = getMethodHandle("co.elastic.apm.agent.impl.transaction.Span", "getType", String.class);

    private static MethodHandle getMethodHandle(String clazz, String method, Class<?> rtype) {
        try {
            return MethodHandles.publicLookup()
                    .findVirtual(Class.forName(clazz), method, MethodType.methodType(rtype));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException ignored) {
            return null;
        }
    }

    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static void onSend(@Advice.FieldValue("session") ClientSessionInternal session) throws Throwable {
        if (isExit == null || getType == null) {
            return;
        }

        Span span = ElasticApm.currentSpan();
        if (span.getId().isEmpty()) {
            return;
        }

        String remoteAddress = session.getConnection().getRemoteAddress();
        if (remoteAddress.startsWith("invm:")) {
            return;
        }

        Object spanImpl = AbstractSpanImplAccessor.getAgentImpl(span);
        if (!(boolean) isExit.invoke(spanImpl) || !"messaging".equals(getType.invoke(spanImpl))) {
            return;
        }

        int startOfPort = remoteAddress.lastIndexOf(':');
        int startOfIP = remoteAddress.lastIndexOf('/');
        int endOfIP = startOfPort >= 0 ? startOfPort : remoteAddress.length();

        if (startOfIP >= 0 && startOfIP < endOfIP) {
            String address = remoteAddress.substring(startOfIP + 1, endOfIP);
            int port = startOfPort >= 0 ? Integer.parseInt(remoteAddress.substring(startOfPort + 1)) : 0;

            span.setDestinationAddress(address, port);
        }
    }
}
