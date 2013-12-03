/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;
import javax.management.JMException;

import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * <h2>Configuration file</h2>
 * 
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and values
 * are separated by equals (=) and the key/value pairs are separated by new
 * lines. The following is a general summary of keys used in the configuration
 * file. For full details on this see the documentation in docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.
 * </li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic unit
 * of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 * 
 */
public class QuorumPeerMain {
	private static final Logger LOG = LoggerFactory
			.getLogger(QuorumPeerMain.class);

	private static final String USAGE = "Usage: QuorumPeerMain configfile";
	private static final String[] ZooArguments = { "./conf/zooQuorum.cfg" };
	public static QuorumPeer quorumPeer;
	public static ServerCnxnFactory cnxnFactory;

	/**
	 * To start the replicated server specify the configuration file name on the
	 * command line.
	 * 
	 * @param args
	 *            path to the configfile
	 */
	public static void main(String[] args) {
		QuorumPeerMain main = new QuorumPeerMain();
		System.setProperty("jute.maxbuffer", "104857600");
		try {
			LOG.info("pgaref Mod starts here!");
			LOG.debug("pgaref debug Mod starts here!");
			main.initializeAndRun(args);
		} catch (IllegalArgumentException e) {
			LOG.error("Invalid arguments, exiting abnormally", e);
			LOG.info(USAGE);
			System.err.println(USAGE);
			System.exit(2);
		} catch (ConfigException e) {
			LOG.error("Invalid config, exiting abnormally", e);
			System.err.println("Invalid config, exiting abnormally");
			System.exit(2);
		} catch (Exception e) {
			LOG.error("Unexpected exception, exiting abnormally", e);
			System.exit(1);
		}
		LOG.info("Exiting normally");
		System.exit(0);
	}

	protected void initializeAndRun(String[] args) throws ConfigException,
			IOException {

		/*
		 * pgaref
		 */
		args = ZooArguments;
		QuorumPeerConfig config = new QuorumPeerConfig();
		Thread mymod = new Thread(new Myclass(1));

		if (args.length == 1) {
			config.parse(args[0]);
		}

		// Start and schedule the the purge task
		DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
				config.getDataDir(), config.getDataLogDir(),
				config.getSnapRetainCount(), config.getPurgeInterval());
		purgeMgr.start();
		mymod.start();
		//if (args.length == 1 && config.servers.size() > 0) {
			runFromConfig(config);
		/*} else {
			LOG.warn("Either no config or no quorum defined in config, running "
					+ " in standalone mode");
			// there is only server in the quorum -- run as standalone

			ZooKeeperServerMain.main(args);

		}*/
		mymod.start();
	}

	public void runFromConfig(QuorumPeerConfig config) throws IOException {
		try {
			ManagedUtil.registerLog4jMBeans();
		} catch (JMException e) {
			LOG.warn("Unable to register log4j JMX control", e);
		}

		LOG.info("Starting quorum peer");
		try {
			cnxnFactory = ServerCnxnFactory.createFactory();
			cnxnFactory.configure(config.getClientPortAddress(),
					config.getMaxClientCnxns());

			quorumPeer = new QuorumPeer();
			quorumPeer.setClientPortAddress(config.getClientPortAddress());
			quorumPeer.setTxnFactory(new FileTxnSnapLog(new File(config
					.getDataLogDir()), new File(config.getDataDir())));
			quorumPeer.setQuorumPeers(config.getServers());
			quorumPeer.setElectionType(config.getElectionAlg());
			quorumPeer.setMyid(config.getServerId());
			quorumPeer.setTickTime(config.getTickTime());
			quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
			quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
			quorumPeer.setInitLimit(config.getInitLimit());
			quorumPeer.setSyncLimit(config.getSyncLimit());
			quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
			quorumPeer.setCnxnFactory(cnxnFactory);
			quorumPeer
					.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
			quorumPeer.setLearnerType(config.getPeerType());

			quorumPeer.start();
			quorumPeer.join();
		} catch (InterruptedException e) {
			// warn, but generally this is ok
			LOG.warn("Quorum Peer interrupted", e);
		}
	}

	/*
	 * pgaref Zookeeper Server Accessor
	 */
	public static ZooKeeperServer getZkServer(ServerCnxnFactory fac) {
		return fac.zkServer;
	}

	public static ServerCnxnFactory getConFactory() {
		return cnxnFactory;
	}

	public static class Myclass implements Runnable {

		int ReqNum;

		public Myclass(int req) {
			LOG.info("pgaref Thread Constr called!");
			ReqNum = req;
		}

		@Override
		public void run() {
			LOG.info("pgaref Thread RUN called!");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				LOG.info("Thead SLEEP Exception");
			}

			/*
			 * Some Notes: Create Sequential 2013-10-18 23:18:21,811 [myid:] -
			 * DEBUG [SyncThread:0:FinalRequestProcessor@88] - Processing
			 * request:: sessionid:0x141cd36ac530000 type:create cxid:0x11
			 * zxid:0x2 txntype:1 reqpath:n/a 2013-10-18 23:18:21,855 [myid:] -
			 * DEBUG [SyncThread:0:FinalRequestProcessor@160] -
			 * sessionid:0x141cd36ac530000 type:create cxid:0x11 zxid:0x2
			 * txntype:1 reqpath:n/a
			 */
			int i = 0;
			while (true) {

				if (QuorumPeerMain.quorumPeer.getServerState()
						.equalsIgnoreCase("LEADING")) {
					LOG.info("pgaref - LEADING!!!!");
					
					
					try{
		                Vote v = null;
		                quorumPeer.setPeerState(ServerState.LOOKING);
		                LOG.info("Going to call leader election: " + i);
		                v = quorumPeer.getElectionAlg().lookForLeader();

		                if (v == null){
		                    Assert.fail("Thread " + i + " got a null vote");
		                }

		                /*
		                 * A real zookeeper would take care of setting the current vote. Here
		                 * we do it manually.
		                 */
		                // Round Robbin voting!
						int voteid = 0;
						if (quorumPeer.getId() == 2)
							voteid = 3;
						else if (quorumPeer.getId() == 3)
							voteid = 1;
						else
							voteid = 2;

						Vote currentVote = new Vote(voteid,
								quorumPeer.getLastLoggedZxid(),
								quorumPeer.getCurrentEpoch());
						
		                quorumPeer.setCurrentVote(currentVote);

		                LOG.info("Finished election: " + i + ", " + v.getId());

		               if(quorumPeer.getPeerState() == ServerState.LEADING){
		            	   
		            	  LOG.info("~~~~~~~Could not change LEADER!!!");
		               }
		            } catch (Exception e) {
		               LOG.info("pgaref ---> Could nor LOOK FOR LEADER!!!!!");
		            }
					/*
					try {
		                Vote v = null;
		                boolean fail = false;

						quorumPeer.setPeerState(ServerState.LOOKING);
						LOG.info("\n~~~~~~~~~~ Going to call leader election ~~~~~~~~~~~~~\n");

						// Round Robbin voting!
						int voteid = 0;
						if (quorumPeer.getId() == 2)
							voteid = 3;
						else if (quorumPeer.getId() == 3)
							voteid = 1;
						else
							voteid = 2;

						Vote currentVote = new Vote(quorumPeer.getId(),
								quorumPeer.getLastLoggedZxid()-10,
								quorumPeer.getCurrentEpoch()-10);

						quorumPeer.setCurrentVote(currentVote);
						v = quorumPeer.getElectionAlg().lookForLeader();
						System.out.println("ENDED LOOKING FOR LEADEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEERRRRRRRRRRRRRRRR");
						
						
					//	quorumPeer.shutdown();
						
						LOG.info("\n~~~~~~~~~~ Leader JUST Voted for " + v + " I will change it to :"+ currentVote);

						if (v == null) {
							LOG.info("\nThread  got a null vote");
						}
						System.out.println("EPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEEEEEEEEEEEEEEEEEEEEEEEEEEEE MREEEEEEEEEEEEEEEEEEEEEE");
						
						LOG.info("\n ------------------------------------------------------------------------Finished election: " + i + ", "
								+ v.getId());

						if ((quorumPeer.getPeerState() == ServerState.LEADING)  && i == 0) {
							fail = true;
							LOG.info("\n I AMM AM STILL LEADER!!!! \n");
							((FastLeaderElection)quorumPeer.getElectionAlg()).shutdown();
							quorumPeer.startLeaderElection();
						}
						else if ((quorumPeer.getPeerState() == ServerState.FOLLOWING)) {
							LOG.info("\n I AM NOT TURNED TO FOLLOWER! \n");
						}
						
						LOG.info("---->>> Master => voted " + v + " My Vote "+ currentVote+" Fail_Var: "
								+ fail);
		            
		            } catch (IOException e) {
						LOG.info("get Current Epoch error!"+e);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					i++;*/
				}
				else {
					LOG.info("pgaref - FOLLOWING!!!!"
							+ QuorumPeerMain.quorumPeer.getServerState());
				}
				try {
					LOG.info("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					LOG.info("pgaref sleeep Thread error!");
				}
			}
		}
	}
}
