/**
 * 
 */
package com.taobao.function.areaTest;

import junit.framework.TestSuite;
import junit.framework.Test;
import junit.textui.TestRunner;

/**
 * @author baoni
 *
 */
public class TestAll extends TestSuite {
	public static Test suit()
	{

		TestSuite suite=new TestSuite("Test All");
		//testcase
//		suite.addTestSuite(Area_00_x_size_Test.class);
		suite.addTestSuite(Area_01_x_areaEliminate_Test.class);
		suite.addTestSuite(Area_02_x_slabBalance_Test.class);
		suite.addTestSuite(Area_03_x_expireEliminate_Test.class);
		suite.addTestSuite(Area_04_x_areaQuotaChange_Test.class);
		return suite;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TestRunner.run(suit());
	}

}
