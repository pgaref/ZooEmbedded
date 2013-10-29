package edu.brown.cs.zkbenchmark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;

import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class AsyncBenchmarkClient extends BenchmarkClient {
	
	TestType _currentType = TestType.UNDEFINED;
	private Boolean _asyncRunning;

	private static final Logger LOG = Logger.getLogger(AsyncBenchmarkClient.class);


	public AsyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}
	
	
	@Override
	protected void submit(int n, TestType type) {
		ListenerContainer<CuratorListener> listeners = (ListenerContainer<CuratorListener>)_client.getCuratorListenable();
		BenchmarkListener listener = new BenchmarkListener(this);
		listeners.addListener(listener);
		_currentType = type;
		_asyncRunning = true;
		
		submitRequests(n, type);
		
		synchronized (_asyncRunning) {
			while (_asyncRunning) {
				try {
					_asyncRunning.wait();
				} catch (InterruptedException e) {
					LOG.warn("AsyncClient #" + _id + " was interrupted", e);
				}
			}
		}

		listeners.removeListener(listener);
	}

	private void submitRequests(int n, TestType type) {
		try {
			submitRequestsWrapped(n, type);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception
			
			//just log the error, not sure how to handle this exception correctly
			LOG.error("Exception while submitting requests", e);
		}
	}

	private void submitRequestsWrapped(int n, TestType type) throws Exception {
		byte data[];

		for (int i = 0; i < n; i++) {
			double time = ((double)System.nanoTime() - _zkBenchmark.getStartTime())/1000000000.0;

			switch (type) {
				case READ:
					_client.getData().inBackground(new Double(time)).forPath(_path);
					break;

				case SETSINGLE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().inBackground(new Double(time)).forPath(
							_path, data);
					break;

				case SETMULTI:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().inBackground(new Double(time)).forPath(
							_path + "/" + (_count % _highestN), data);
					break;

				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().inBackground(new Double(time)).forPath(
							_path + "/" + _count, data);
					_highestN++;
					break;

				case DELETE:
					_client.delete().inBackground(new Double(time)).forPath(_path + "/" + _count);
					_highestDeleted++;

					if (_highestDeleted >= _highestN) {
						zkAdminCommand("stat");							
						_zkBenchmark.notifyFinished(_id);
						_timer.cancel();
						_count++;
						return;
					}
			}
			_count++;
		}

	}
	
	public void CreateInternal(String blockname, byte [] data){
		//String blockname  = "/foo";
		//String data = "pgaref";
		int i = 0;
		//pgaref -> 23 is the byte len of ZooDefs.Ids.OPEN_ACL_UNSAFE
		 int DataHeaderLength = 16  + blockname.length() + data.length +23;
		 //ByteBuffer Requestdata = ByteBuffer.allocate(DataHeaderLength);
		 ByteBuffer Requestdata = ByteBuffer.wrap(new byte [DataHeaderLength]);
		 try{
			 
			 Requestdata.clear();
			 //path name len
			 Requestdata.putInt((blockname.length()));
			 //path name
			 Requestdata.put(blockname.getBytes());
			 //data len
			 Requestdata.putInt(data.length);
			  //data 
			 Requestdata.put(data);
			 //acl null
			 Requestdata.putInt(ZooDefs.Ids.OPEN_ACL_UNSAFE.size());
			 for(int  index = 0 ; index < ZooDefs.Ids.OPEN_ACL_UNSAFE.size();  index++ ){
				 org.apache.zookeeper.data.ACL e1 = ZooDefs.Ids.OPEN_ACL_UNSAFE.get(index);
				 ByteArrayOutputStream baos = new ByteArrayOutputStream();
	             OutputArchive boa = BinaryOutputArchive.getArchive(baos);
	             boa.writeRecord(e1, null);
				 Requestdata.put(baos.toByteArray());
			 }
			 
			 
				 /* ByteArrayOutputStream baos = new ByteArrayOutputStream();
				  DataOutputStream dos = new DataOutputStream(baos);
				  BinaryOutputArchive archive = new BinaryOutputArchive(dos);
				  try {
					ZooDefs.Ids.OPEN_ACL_UNSAFE.get(0).serialize(archive,"");
					
				} catch (IOException e) {
					LOG.info("serialization Exception: "+ e);
				}
			      Requestdata.put(archive.toString().getBytes());
				  */	
		      //the flags
			 Requestdata.putInt(CreateMode.PERSISTENT_SEQUENTIAL.toFlag());
			 Requestdata.flip();
		 }catch(IOException ex){
			 LOG.info("pgaref - Exception Serializing ACL List");	 
		 }catch(BufferOverflowException ex){
			 LOG.info("BufferOverflowException: "+ex);
		 }
		 
		  /* DATA End here */
		  
		  long zxid = ZxidUtils.makeZxid(1, i);
		  TxnHeader hdr = new TxnHeader(1, 10+i, zxid, 30+i,ZooDefs.OpCode.create); 
		  Record txn = new CreateTxn("/foo" + i, "pgaref".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 1); 
		  Request req = new Request(null, 2285l, 1, OpCode.create, Requestdata,null);  
		  req.hdr = hdr; 
		  req.txn = txn;
		  
	   /* CreateRequest createRequest = new CreateRequest();
		  try {
			ByteBufferInputStream.byteBuffer2Record(req.request, createRequest);
		} catch (IOException e1) {
			LOG.info("pgaref -Serialization request Known error");
		}
		  
		 LOG.info("pgaref - Lets see : " +  createRequest.toString() + " Path: " + createRequest.getPath() + " Data: " + createRequest.getData().toString() +
		 " ACL: "+  createRequest.getAcl().toString() + " Flags: "+ createRequest.getFlags()); 
		*/  
		  //FOR QUORUM
		 // QuorumPeerMain.quorumPeer.getActiveServer().submitRequest(req);
		 
		  // FOR STANDALONE SERVER
		  try {
			ZooKeeperServer.finalProcessor.processRequest(req);
			} catch (RequestProcessorException e) {
				LOG.debug("pgaref request error"+ e);
			}
			LOG.info("is going to process!!!");
	}
	
	@Override
	protected void finish() {
		synchronized (_asyncRunning) {
			_asyncRunning.notify();
			_asyncRunning = false;
		}
	}

	@Override
	protected void resubmit(int n) {
		submitRequests(n, _currentType);
	}
}
