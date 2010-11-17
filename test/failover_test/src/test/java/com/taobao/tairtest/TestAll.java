/**
 * 
 */
package com.taobao.tairtest;

import junit.framework.TestSuite;
import junit.framework.Test;
import junit.textui.TestRunner;

/**
 * @author dongpo
 *
 */
public class TestAll extends TestSuite {
	public static Test suit()
	{
//		copycount大于1的容灾
		TestSuite suite=new TestSuite("Test All");
		suite.addTestSuite(FailOverConfigServerTest.class);
		suite.addTestSuite(FunctionDataServerTest.class);
		suite.addTestSuite(FailOverDataServerTest.class);
		suite.addTestSuite(FailoverClusterTest.class);
		
//		copycount为1的容灾
		suite.addTestSuite(FailOverConfigServerTest1.class);
//		suite.addTestSuite(FunctionDataServerTest1.class);
//		suite.addTestSuite(FailOverDataServerTest1.class);
//		suite.addTestSuite(FailoverClusterTest1.class);
		return suite;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TestRunner.run(suit());
	}

}
