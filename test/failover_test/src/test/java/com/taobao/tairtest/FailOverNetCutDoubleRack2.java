/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailOverNetCutDoubleRack2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_cutoff_a1b1_a1b2_b1a2() {
		log.info("start netcut test Failover case 01");
		int datacnt1, datacnt2, datacnt3;

		// start cluster
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// 1st write data while shut off net
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0);
		log.info("Write data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0);
		log.info("Write data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0);
		log.info("Write data success on client!");

		// wait for duplicate
		waitto(10);

		// 1st read data while shut off net
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1);
		log.info("Get data success on client!");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// 2ed write data while shut off net
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data over on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Write data over on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Write data over on client!");

		// wait for duplicate
		waitto(10);

		// 2ed read data while shut off net
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.9);
		log.info("Get data success on client!");

		// end test
		log.info("end netcut test Failover case 01");
	}

	@Test
	public void testFailover_02_cutoff_a1b1_b1a2_a1b2() {
		log.info("start netcut test Failover case 02");
		int datacnt1, datacnt2, datacnt3;

		// start cluster
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);

		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// write data while shut off net
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data over on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Write data over on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Write data over on client!");

		// wait for duplicate
		waitto(10);

		// read data while shut off net
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.9);
		log.info("Get data success on client!");

		// end test
		log.info("end netcut test Failover case 02");
	}

	@Test
	public void testFailover_03_cutoff_b1a2_a1b1_a1b2() {
		log.info("start netcut test Failover case 03");
		int datacnt1, datacnt2, datacnt3;

		// start cluster
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);

		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// write data while shut off net
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data over on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Write data over on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Write data over on client!");

		// wait for duplicate
		waitto(10);

		// read data while shut off net
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.9);
		log.info("Get data success on client!");

		// end test
		log.info("end netcut test Failover case 03");
	}

	@Test
	public void testFailover_04_cutoff_a1b2_b1a2_a1b1() {
		log.info("start netcut test Failover case 04");
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on master_cs
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a2b1
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data from client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Cilent!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on slave cs!");

		// read the data from client, master cs, slave cs
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("get data success on client!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on slave cs!");

		log.info("end netcut test Failover case 04");
	}

	@Test
	public void testFailover_05_cutoff_b1a2_a1b1_a1b2() {
		log.info("start netcut test Failover case 05");
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on master_cs
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on Cilent!");
		// wait for duplicate
		waitto(10);

		// /////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: all
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data from client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Cilent!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.8);
		log.info("Write data success on slave cs!");

		// read the data from client,master cs,slave cs
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("get data success on client!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.8);
		log.info("get data success on slave cs!");

		log.info("start netcut test Failover case 05");
	}

	@Test
	public void testFailover_06_cutoff_b1a2_a1b2_a1b1() {
		log.info("start netcut test Failover case 06");
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on Cilent!");
		// wait for duplicate
		waitto(10);

		// /////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: all
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data from client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master CS!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on Backup CS!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Cilent!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Backup CS!");

		// read the data from client,master CS,Backup CS
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("get data success on client!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on slave cs!");

		log.info("end netcut test Failover case 06");
	}

	@Test
	public void testFailover_07_cutoff_a1b1_b1a2_a1b2_recover_a1b1_a1b2_b1a2() {
		log.info("start netcut test Failover case 07");
		int datacnt1, datacnt2, datacnt3;

		// start cluster
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);

		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// write data while shut off net
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data over on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Write data over on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Write data over on client!");

		// wait for duplicate
		waitto(10);

		// read data while shut off net
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.9);
		log.info("Get data success on client!");

		log.info("////////////////////////////////////");
		log.info("/////////// recover net ////////////");
		log.info("////////////////////////////////////");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b1
		recover_net(csList.get(0));
		recover_net(csList.get(1));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read 1st time after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.6);
		log.info("Get data success on master cs!");
		// datacnt2=getVerifySuccessful(csList.get(1));
		// assertTrue("verify data failed!",datacnt2/put_count_float>=0);
		// log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.3);
		log.info("Get data success on client!");

		// put 1st time after recover
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// put))fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Put data success on master cs!");
		// datacnt2=getVerifySuccessful(csList.get(1));
		// assertTrue("verify data failed!",datacnt2/put_count_float>0.9);
		// log.info("Put data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Put data success on client!");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b2
		recover_net(dsList2.get(3));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: b1a2
		recover_net(dsList2.get(2));
		recover_net(dsList2.get(4));
		recover_net(dsList2.get(5));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read 2ed time after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.5);
		log.info("Get data success on master cs!");
		// datacnt2=getVerifySuccessful(csList.get(1));
		// assertTrue("verify data failed!",datacnt2/put_count_float>0.9);
		// log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 2.2);
		log.info("Get data success on client!");

		clean_tool(csList.get(0));
		clean_tool(csList.get(1));
		clean_tool(local);

		// put after recover all
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Put data success on client!");

		// end test
		log.info("end netcut test Failover case 07");
	}

	@Test
	public void testFailover_08_cutoff_b1a2_a1b1_a1b2_recover_a1b1_b1a2_a1b2() {
		log.info("start netcut test Failover case 08");
		int datacnt1, datacnt2, datacnt3;

		// start cluster
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);

		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// write data while shut off net
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data over on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Write data over on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Write data over on client!");

		// wait for duplicate
		waitto(10);

		// read data while shut off net
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.9);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.9);
		log.info("Get data success on client!");

		log.info("////////////////////////////////////");
		log.info("/////////// recover net ////////////");
		log.info("////////////////////////////////////");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b1
		recover_net(csList.get(0));
		recover_net(csList.get(1));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read 1st time after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.6);
		log.info("Get data success on master cs!");
		// datacnt2=getVerifySuccessful(csList.get(1));
		// assertTrue("verify data failed!",datacnt2/put_count_float>=0);
		// log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.3);
		log.info("Get data success on client!");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: b1a2
		recover_net(dsList2.get(2));
		waitto(down_time);

		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b2
		recover_net(dsList2.get(3));
		recover_net(dsList2.get(4));
		recover_net(dsList2.get(5));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read 2ed time after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.6);
		log.info("Get data success on master cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.3);
		log.info("Get data success on client!");

		clean_tool(csList.get(0));
		clean_tool(csList.get(1));
		clean_tool(local);

		// put after recover all
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Put data success on client!");

		// end test
		log.info("end netcut test Failover case 08");
	}

	@Test
	public void testFailover_09_cutoff_a1b2_b1a2_a1b1_recover_a1b2_a1b1_b1a2() {
		log.info("start netcut test Failover case 09");

		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on master_cs
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a2b1
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data from client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Cilent!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on slave cs!");

		// read the data from client, master cs, slave cs
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("get data success on client!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on slave cs!");

		log.info("////////////////////////////////////");
		log.info("/////////// recover net ////////////");
		log.info("////////////////////////////////////");

		// ///////////////////////////////////////////////////////////////////////////
		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");
		// recover net: a1b2
		recover_net(csList.get(0));
		recover_net(dsList2.get(3));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		// slave cs will receive a few package from master cs
		// wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b1
		recover_net(csList.get(0));
		recover_net(csList.get(1));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		// slave cs may be linked to master cs, then will not rebuild
		// wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: b1a2
		recover_net(csList.get(1));
		recover_net(dsList2.get(2));
		waitto(down_time);
		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: all
		recover_net(dsList2.get(4));
		recover_net(dsList2.get(5));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("Get data success on client!");

		clean_tool(csList.get(0));
		clean_tool(csList.get(1));
		clean_tool(local);

		// put after recover all
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Put data success on client!");

		log.info("end netcut test Failover case 09");
	}

	@Test
	public void testFailover_10_cutoff_b1a2_a1b1_a1b2_recover_a1b2_b1a2_a1b1() {
		log.info("start netcut test Failover case 10");

		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on master_cs
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on Cilent!");
		// wait for duplicate
		waitto(10);

		// /////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: all
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data from client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Cilent!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on slave cs!");

		// read the data from client,master cs,slave cs
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("get data success on client!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on slave cs!");

		log.info("////////////////////////////////////");
		log.info("/////////// recover net ////////////");
		log.info("////////////////////////////////////");

		// ///////////////////////////////////////////////////////////////////////////
		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");
		// recover net: a1b2
		recover_net(csList.get(0));
		recover_net(dsList2.get(3));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		// wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// recover net: b1a2
		recover_net(csList.get(1));
		recover_net(dsList2.get(2));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		// wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// recover all
		recover_net(dsList2.get(4));
		recover_net(dsList2.get(5));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("Get data success on client!");

		clean_tool(csList.get(0));
		clean_tool(csList.get(1));
		clean_tool(local);

		// put after recover all
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Put data success on client!");

		log.info("end netcut test Failover case 10");
	}

	@Test
	public void testFailover_11_cutoff_b1a2_a1b1_a1b2_recover_b1a2_a1b1_a1b2() {
		log.info("start netcut test Failover case 11");

		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on Cilent!");
		// wait for duplicate
		waitto(10);

		// /////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		waitto(ds_down_time);
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// /////////////////////////////////////////////////////////////////
		// shut off: all
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);
		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data from client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master CS!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on Backup CS!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Cilent!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Write data success on Backup CS!");

		// read the data from client,master CS,Backup CS
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		wait_tool_on_mac(local);
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));

		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1.9);
		log.info("get data success on client!");
		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on master cs!");
		datacnt1 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("get data success on slave cs!");

		log.info("////////////////////////////////////");
		log.info("/////////// recover net ////////////");
		log.info("////////////////////////////////////");

		// ///////////////////////////////////////////////////////////////////
		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");
		// recover net: b1a2
		recover_net(csList.get(1));
		recover_net(dsList2.get(2));
		waitto(down_time);
		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////
		// recover net: a1b1
		recover_net(csList.get(0));
		recover_net(csList.get(1));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////
		// recover net: a1b2
		recover_net(csList.get(0));
		recover_net(dsList2.get(3));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// recover all
		recover_net(dsList2.get(4));
		recover_net(dsList2.get(5));
		waitto(down_time);
		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Get data success on master cs!");
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 1);
		log.info("Get data success on client!");

		clean_tool(csList.get(0));
		clean_tool(csList.get(1));
		clean_tool(local);

		// put after recover all
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Put data success on client!");

		log.info("end netcut test Failover case 11");
	}

	@Test
	public void testFailover_12_cutoff_a1b1_a1b2_b1a2_recover_b1a2_a1b2_a1b1() {
		log.info("start netcut test Failover case 12");
		int datacnt1, datacnt2, datacnt3;

		// start cluster
		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on client!");
		// wait for duplicate
		waitto(10);

		int migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		int migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b1
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// shut off: b1a2
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);
		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read data while shut off net
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(local);

		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Get data success on client!");

		log.info("////////////////////////////////////");
		log.info("/////////// recover net ////////////");
		log.info("////////////////////////////////////");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: b1a2
		recover_net(csList.get(1));
		recover_net(dsList2.get(2));
		waitto(down_time);

		wait_keyword_equal(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read 1st time after recover
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(local);

		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.9);
		log.info("Get data success on client!");

		// put 1st time after recover
		if (!execute_tool_on_mac(csList.get(0), put))
			fail("put fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), put))
			fail("put fail on slave cs!");
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.6);
		log.info("Put data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.7);
		log.info("Put data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 0.6);
		log.info("Put data success on client!");

		// read 2st time after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		if (!execute_tool_on_mac(csList.get(1), get))
			fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.6);
		log.info("Get data success on master cs!");
		datacnt2 = getVerifySuccessful(csList.get(1));
		assertTrue("verify data failed!", datacnt2 / put_count_float > 0.7);
		log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.5);
		log.info("Get data success on client!");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b2
		recover_net(csList.get(0));
		recover_net(dsList2.get(3));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// ///////////////////////////////////////////////////////////////////////////
		// recover net: a1b1 and all
		recover_net(dsList2.get(4));
		recover_net(dsList2.get(5));
		waitto(down_time);

		wait_keyword_change(csList.get(0), start_migrate, migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, migrateCount2);
		migrateCount1 = check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log");
		migrateCount2 = check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log");

		// read 2ed time after recover
		if (!execute_tool_on_mac(csList.get(0), get))
			fail("get fail on master cs!");
		// if(!execute_tool_on_mac(csList.get(1),
		// get))fail("get fail on slave cs!");
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(csList.get(0));
		// wait_tool_on_mac(csList.get(1));
		wait_tool_on_mac(local);

		datacnt1 = getVerifySuccessful(csList.get(0));
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.6);
		log.info("Get data success on master cs!");
		// datacnt2=getVerifySuccessful(csList.get(1));
		// assertTrue("verify data failed!",datacnt2/put_count_float>=0);
		// log.info("Get data success on slave cs!");
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1.5);
		log.info("Get data success on client!");

		clean_tool(csList.get(0));
		clean_tool(csList.get(1));
		clean_tool(local);

		// put after recover all
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		datacnt3 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt3 / put_count_float > 1);
		log.info("Put data success on client!");

		// end test
		log.info("end netcut test Failover case 12");
	}

	@Test
	public void testFailover_13_cutoff_a1b2_recover_before_rebuild() {
		log.info("start netcut test Failover case 13");

		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on Client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on Cilent!");
		// wait for duplicate
		waitto(10);

		// shut off: a1b2
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		// waitto(ds_down_time/4);

		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);

		// recover_net:a1b2
		recover_net(csList.get(0));
		recover_net(dsList2.get(3));
		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);
		waitto(down_time);

		wait_keyword_equal(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_equal(csList.get(1), start_migrate, init_migrateCount2);

		// read the data from Client
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("read data success on Client!");

		log.info("end netcut test Failover case 13");
	}

	@Test
	public void testFailover_14_cut_all_flash() {
		log.info("start netcut test Failover case 14");

		if (!control_cluster(csList, dsList2, start, 0))
			fail("start cluster failure!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		int init_migrateCount1, init_migrateCount2;
		init_migrateCount1 = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");
		init_migrateCount2 = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		// write data on client
		if (!execute_tool_on_mac(local, put))
			fail("put fail on client!");
		wait_tool_on_mac(local);
		int datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float == 1);
		log.info("Write data success on Cilent!");
		// wait for duplicate
		waitto(10);

		// shut off: all
		shutoff_net(csList.get(0), csport, csList.get(1), csport);
		shutoff_net(csList.get(0), csport, dsList2.get(3), dsport);
		shutoff_net(csList.get(0), csport, dsList2.get(5), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(2), dsport);
		shutoff_net(csList.get(1), csport, dsList2.get(4), dsport);

		waitto(ds_down_time);

		wait_keyword_change(csList.get(0), start_migrate, init_migrateCount1);
		wait_keyword_change(csList.get(1), start_migrate, init_migrateCount2);

		// /////////////////////////////////////////////////////////////////////////////////////////////////
		// read the data from client
		if (!execute_tool_on_mac(local, get))
			fail("get fail on client!");
		wait_tool_on_mac(local);
		datacnt1 = getVerifySuccessful(local);
		assertTrue("verify data failed!", datacnt1 / put_count_float > 0.9);
		log.info("Read data success on client!");

		log.info("end netcut test Failover case 14");
	}

	@BeforeClass
	public static void subBeforeClass() {
		if (!batch_modify(csList, tair_bin + groupconf, copycount, "2"))
			fail("modify configure file failure");
		if (!batch_modify(dsList, tair_bin + groupconf, copycount, "2"))
			fail("modify configure file failure");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		clean_bin_tool(csList.get(0));
		clean_bin_tool(csList.get(1));
		clean_bin_tool(local);
		reset_cluster(csList, dsList2);
		batch_uncomment(csList, tair_bin + groupconf, dsList2, "#");
		execute_shift_tool(local, "conf6");
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
		clean_bin_tool(csList.get(0));
		clean_bin_tool(csList.get(1));
		clean_bin_tool(local);
		reset_cluster(csList, dsList2);
		batch_uncomment(csList, tair_bin + groupconf, dsList2, "#");
		if (!batch_recover_net(dsList2))
			fail("recover net failure");
	}
}
