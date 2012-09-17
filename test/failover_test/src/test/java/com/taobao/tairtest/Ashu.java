/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Ashu extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_all_server() {
		log.info("start cluster test failover case 01");
		log.info("end cluster test failover case 01");
	}

	@Test
	public void testFailover_02_restart_all_server_after_join_ds_and_migration() {
		log.info("start cluster test failover case 02");
		log.info("end cluster test failover case 02");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
	}
}
