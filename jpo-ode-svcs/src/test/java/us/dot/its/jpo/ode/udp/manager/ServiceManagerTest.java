package us.dot.its.jpo.ode.udp.manager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.junit.Test;

import mockit.Capturing;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import us.dot.its.jpo.ode.dds.AbstractSubscriberDepositor;
import us.dot.its.jpo.ode.udp.AbstractUdpReceiverPublisher;

public class ServiceManagerTest {

   @Tested
   ServiceManager testServiceManager;

   @Injectable
   ThreadFactory mockThreadFactory;

   @Test
   public void receiverSubmitCallsExecutorService(@Capturing Executors mockExecutors,
         @Injectable AbstractUdpReceiverPublisher mockAbstractUdpReceiverPublisher,
         @Mocked ExecutorService mockExecutorService) {

      new Expectations() {
         {
            Executors.newSingleThreadExecutor((ThreadFactory) any);
            result = mockExecutorService;

            mockExecutorService.submit((AbstractUdpReceiverPublisher) any);
         }
      };

      testServiceManager.submit(mockAbstractUdpReceiverPublisher);

   }

   public void depositorCallsSubscribe(@Mocked AbstractSubscriberDepositor<?, ?> mockAbstractSubscriberDepositor) {

      new Expectations() {
         {
            mockAbstractSubscriberDepositor.subscribe(anyString);
         }
      };

      testServiceManager.submit(mockAbstractSubscriberDepositor);
   }

}
