package org.apache.cassandra.sidecar;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the {@link MainModule} class
 */
public class MainModuleTest {
    @Test
    public void testSidecarVersion()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));

        String sidecarVersion = injector.getInstance(Key.get(String.class, Names.named("SidecarVersion")));

        assertEquals("1.0-TEST", sidecarVersion);
    }
}
