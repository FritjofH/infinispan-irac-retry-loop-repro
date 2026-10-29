package repro;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.infinispan.Cache;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracExecutor;
import org.infinispan.xsite.irac.IracManager;

/**
 * Site A (no store, max-count 10) backs up ASYNC to site B. Site B is not running while keys are
 * written, so the keys stay queued in IRAC and most are evicted before IRAC can send them.
 * Site B is then started, and the IRAC queue of site A is watched.
 */
public final class IracRetryLoopRepro {

   private static final int KEYS = Integer.getInteger("keys", 50);
   private static final int DOWN_SECONDS = Integer.getInteger("down.seconds", 5);
   private static final int UP_SECONDS = Integer.getInteger("up.seconds", 30);
   /** Steady state = the last N seconds of a phase, after IRAC's randomized back-off (500 ms doubling) has expired. */
   private static final int STEADY_SECONDS = 10;

   private static final com.sun.management.OperatingSystemMXBean OS =
         (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

   public static void main(String[] args) throws Exception {
      System.setProperty("java.net.preferIPv4Stack", "true");
      System.setProperty("jgroups.bind.address", "127.0.0.1");
      System.out.printf(Locale.ROOT, "Infinispan %s, JDK %s, %d CPUs%n", Version.getVersion(),
            System.getProperty("java.version"), Runtime.getRuntime().availableProcessors());

      EmbeddedCacheManager siteA = startSite("A", "B", 7800, 7840);
      Cache<String, String> cache = siteA.getCache("repro");

      DefaultIracManager irac = (DefaultIracManager) ComponentRegistry.componentOf(cache, IracManager.class);
      Map<?, ?> pendingKeys = (Map<?, ?>) field(irac, "updatedKeys");
      LongAdder passes = countIracPasses((IracExecutor) field(irac, "iracExecutor"));

      for (int i = 0; i < KEYS; i++) {
         cache.put("key-" + i, "value-" + i);
      }
      System.out.printf(Locale.ROOT, "Wrote %d keys. Entries in memory: %d. Keys queued for IRAC: %d%n",
            KEYS, cache.getAdvancedCache().getDataContainer().size(), pendingKeys.size());

      Result down = measure("site B down", DOWN_SECONDS, pendingKeys, passes);

      EmbeddedCacheManager siteB = startSite("B", "A", 7820, 7860);
      siteB.getCache("repro");
      awaitSiteView(siteA, "B");
      Result up = measure("site B up", UP_SECONDS, pendingKeys, passes);

      System.out.println();
      System.out.println(down);
      System.out.println(up);
      System.out.println(verdict(up));

      siteB.stop();
      siteA.stop();
      System.exit(0);
   }

   private static EmbeddedCacheManager startSite(String site, String remote, int localPort, int bridgePort) throws IOException {
      String xml;
      try (InputStream in = IracRetryLoopRepro.class.getResourceAsStream("/site-template.xml")) {
         xml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      xml = xml.replace("@SITE@", site).replace("@REMOTE@", remote)
            .replace("@LOCAL_PORT@", String.valueOf(localPort)).replace("@BRIDGE_PORT@", String.valueOf(bridgePort));
      ConfigurationBuilderHolder holder = new ParserRegistry().parse(xml);
      return new DefaultCacheManager(holder, true);
   }

   /** Every IRAC send pass is one task on the IRAC executor; count them. */
   private static LongAdder countIracPasses(IracExecutor iracExecutor) {
      LongAdder passes = new LongAdder();
      Executor delegate = iracExecutor.executor();
      iracExecutor.setExecutor(task -> {
         passes.increment();
         delegate.execute(task);
      });
      return passes;
   }

   private static Result measure(String phase, int seconds, Map<?, ?> pendingKeys, LongAdder passes) throws InterruptedException {
      System.out.printf(Locale.ROOT, "--- %s: measuring for %d s ---%n", phase, seconds);
      long[] passesAt = new long[seconds + 1];
      long[] cpuAt = new long[seconds + 1];
      long[] wallAt = new long[seconds + 1];
      passesAt[0] = passes.sum();
      cpuAt[0] = OS.getProcessCpuTime();
      wallAt[0] = System.nanoTime();
      for (int s = 1; s <= seconds; s++) {
         TimeUnit.SECONDS.sleep(1);
         passesAt[s] = passes.sum();
         cpuAt[s] = OS.getProcessCpuTime();
         wallAt[s] = System.nanoTime();
         System.out.printf(Locale.ROOT, "  t=%2ds  pending IRAC keys=%3d  IRAC passes/s=%,8d  process CPU=%.2f cores%n", s,
               pendingKeys.size(), passesAt[s] - passesAt[s - 1], cores(cpuAt, wallAt, s - 1, s));
      }
      int from = Math.max(0, seconds - STEADY_SECONDS);
      double passesPerSecond = (passesAt[seconds] - passesAt[from]) * 1e9 / (wallAt[seconds] - wallAt[from]);
      return new Result(phase, seconds - from, pendingKeys.size(), passesPerSecond, cores(cpuAt, wallAt, from, seconds));
   }

   private static double cores(long[] cpu, long[] wall, int from, int to) {
      return (cpu[to] - cpu[from]) / (double) (wall[to] - wall[from]);
   }

   private static String verdict(Result up) {
      String numbers = String.format(Locale.ROOT, " (%d keys still queued, %,.0f IRAC passes/s, process CPU %.2f cores)",
            up.pendingAtEnd, up.passesPerSecond, up.cpuCores);
      String head = "VERDICT Infinispan " + Version.getVersion() + ": ";
      if (up.pendingAtEnd == 0) {
         return head + "NO LOOP - IRAC queue drained after site B came up" + numbers;
      }
      if (up.passesPerSecond > 1000) {
         return head + "RETRY LOOP - evicted keys never drain and IRAC re-runs with nothing to send" + numbers;
      }
      return head + "UNCLEAR - keys still queued but IRAC is not spinning" + numbers;
   }

   private static void awaitSiteView(EmbeddedCacheManager cm, String site) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (!transport(cm).getSitesView().contains(site)) {
         if (System.nanoTime() > deadline) {
            throw new IllegalStateException("site " + site + " not in sites view: " + transport(cm).getSitesView());
         }
         TimeUnit.MILLISECONDS.sleep(100);
      }
      System.out.println("Site B is in site A's sites view: " + transport(cm).getSitesView());
   }

   private static Transport transport(EmbeddedCacheManager cm) {
      return GlobalComponentRegistry.componentOf(cm, Transport.class);
   }

   private static Object field(Object target, String name) throws ReflectiveOperationException {
      Field f = target.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(target);
   }

   private record Result(String phase, int window, int pendingAtEnd, double passesPerSecond, double cpuCores) {
      @Override
      public String toString() {
         return String.format(Locale.ROOT, "%-12s pending at end=%d  last %d s: IRAC passes/s=%,.0f  process CPU=%.2f cores",
               phase + ":", pendingAtEnd, window, passesPerSecond, cpuCores);
      }
   }
}
