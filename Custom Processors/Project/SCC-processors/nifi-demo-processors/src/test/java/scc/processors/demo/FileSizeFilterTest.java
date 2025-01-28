package scc.processors.demo;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class FileSizeFilterTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(FileSizeFilter.class);
    }

    @Test
    public void testProcessor() {

    }

}
