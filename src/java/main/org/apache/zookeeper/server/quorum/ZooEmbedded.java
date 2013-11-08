package org.apache.zookeeper.server.quorum;

public interface ZooEmbedded {
	
	/* Create Configuration Files and Classes */
	public void init();

	/* Starts Server thread (call init internally) */
	public void start();

	/* Stops thread */
	public void stop();
	
	/* Inserts a internal block with fake session and socket connection to avoid overhead 
	 * this block by default is persistent sequential with ACL = no security 
	 */
	//public static void insertPersistent(String blockname, byte[] data);
	
	/* Delete a spefic znode */
	public void delete(String blockname);
	
	/* Periodic cleanup of znodes  to avoid memory FULL */
	public void memoryCleanup();
	
	/* Get Server State : 
	 * For one server: Standalone 
	 * For quorum: Following or Leading
	 */
	public String getServerState();

}
