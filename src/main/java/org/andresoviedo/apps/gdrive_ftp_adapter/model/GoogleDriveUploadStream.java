package org.andresoviedo.apps.gdrive_ftp_adapter.model;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.andresoviedo.apps.gdrive_ftp_adapter.model.GoogleDrive.GFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.ParentReference;


public class GoogleDriveUploadStream extends OutputStream {

	private static int DEFAULT_UPLOAD_CHUNK_SIZE = 256 * 1024;   // has to be a multiple of 256k

	private static final Log logger = LogFactory.getLog(GoogleDriveUploadStream.class);
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private final Drive drive;
	private byte[] uploadCache;
	private int cacheOffset = 0;
	private long length = 0;
	private GFile gfile;
	
	private String mimeType = "application/octet-stream";
	private String uploadUri = null;
	private File uploadedFile = null;
	
	private long startTime = 0;

	public GoogleDriveUploadStream(Drive drive, GFile gfile) {
		this.drive = drive;
		this.gfile = gfile;
		uploadCache = new byte[DEFAULT_UPLOAD_CHUNK_SIZE];
	}

	@Override
	public void write(int b) throws IOException {

		uploadCache[cacheOffset++] = (byte) b;

		if (cacheOffset == uploadCache.length) {
			handleChunk();
			if (startTime > 0) {
				long endTime = System.nanoTime();
				try {
					int kfactor = (int) (((long)uploadCache.length * 3000 / ((endTime-startTime)/1000000)) / (256*1024));
					if (kfactor < 1) kfactor = 1;			// 256 kB/s
					if (kfactor > 200) kfactor = 200;		//  50 MB/s
					DEFAULT_UPLOAD_CHUNK_SIZE = kfactor*256*1024;
				}
				catch (Exception e){
					// over/underflow on very slow/fast connection maybe possible => ignore
				}
				int newCacheSize = DEFAULT_UPLOAD_CHUNK_SIZE;
				// only change cache size if > 20% difference
				if (newCacheSize != uploadCache.length && Math.abs((long)newCacheSize-uploadCache.length)*100/uploadCache.length > 20) {
					logger.info("Changing upload cache size to " + newCacheSize/1024 + " kBytes");
					uploadCache = new byte[newCacheSize];
				}
			}
			startTime = System.nanoTime();
		}
	}

	@Override
	public void flush() throws IOException {

	}

	@Override
	public void close() throws IOException {
		handleChunk();
	}
	
	
	private void handleChunk() throws IOException {
		if (uploadedFile != null) return;
		if (length == 0) {
			// first chunk
			mimeType = guessMimeType();
			if (cacheOffset < uploadCache.length) {
				// small file -> upload without resumable session
				File f = buildFileMetadata();
				AbstractInputStreamContent fileContent = new ByteArrayContent(mimeType, uploadCache, 0, cacheOffset);
				if (gfile.isExists()) {
					uploadedFile = drive.files().update(gfile.getId(), f, fileContent).execute();
				} else {
					uploadedFile = drive.files().insert(f, fileContent).execute();			
				}
			} else {
				getUploadSession();
				uploadChunk(cacheOffset == 0);
			}
		} else {
			uploadChunk(cacheOffset == 0);
		}
	}
	
	
	private String guessMimeType() throws IOException {
		Metadata meta = new Metadata();
		meta.add(Metadata.RESOURCE_NAME_KEY, gfile.getName());
		//AutoDetectParser parser = new AutoDetectParser();
		//Detector detector = parser.getDetector();
		Detector detector = TikaConfig.getDefaultConfig().getDetector();
		//Detector detector = new DefaultDetector();
		MediaType m = detector.detect(new ByteArrayInputStream(uploadCache), meta);
		logger.info("Detected MIME type " + mimeType);
		return m.toString();
	}
	
	
	
	private File buildFileMetadata() {
		File file = new File();
		file.setTitle(gfile.getName());
		file.setModifiedDate(new DateTime(gfile.getLastModified() != 0 ? gfile.getLastModified() : System.currentTimeMillis()));

		List<ParentReference> newParents = new ArrayList<ParentReference>(1);
		if (gfile.getParents() != null) {
			for (String parent : gfile.getParents()) {
				newParents.add(new ParentReference().setId(parent));
			}
			file.setParents(newParents);
		} else if (gfile.getCurrentParent() != null) {
			newParents = Collections.singletonList(new ParentReference().setId(gfile.getCurrentParent().getId()));
			file.setParents(newParents);
		}
		return file;
	}
	
	
	private void getUploadSession() throws IOException {
		if (uploadUri != null) return;
		
		// Step 1: Start a resumable session
		File metadata = buildFileMetadata();
		String requestUri;
		HttpRequest request;
		if (gfile.isExists()) {
			requestUri = drive.getRootUrl() + "upload/drive/v2/files/" + gfile.getId() + "?uploadType=resumable&modifiedDateBehavior=fromBodyOrNow";
			request = drive.getRequestFactory().buildPutRequest(new GenericUrl(requestUri), new JsonHttpContent(JSON_FACTORY, metadata));
		} else {
			requestUri = drive.getRootUrl() + "upload/drive/v2/files?uploadType=resumable";
			request = drive.getRequestFactory().buildPostRequest(new GenericUrl(requestUri), new JsonHttpContent(JSON_FACTORY, metadata));
		}
		request.getHeaders().set("X-Upload-Content-Type", mimeType);
		HttpResponse response = request.execute();
		
		// Step 2: Save the resumable session URI
		if (response.getStatusCode() == 200) {
			uploadUri = response.getHeaders().getLocation();
			logger.debug("Created resumable upload session: " + uploadUri);
		} else {
			throw new IOException("Error while initializing upload session");
		}
	}
	
	private void uploadChunk(boolean close) throws IOException {
		HttpContent content = new ByteArrayContent(mimeType, uploadCache, 0, cacheOffset);
		HttpRequest request = drive.getRequestFactory().buildPutRequest(new GenericUrl(uploadUri), content);
		request.getHeaders().setContentRange("bytes " + (cacheOffset == 0 ? "*" : length + "-" + (length+cacheOffset-1)) + "/" + (close || cacheOffset < uploadCache.length ? (length+cacheOffset) : "*"));
		request.setThrowExceptionOnExecuteError(false);
		HttpResponse response = request.execute();
		logger.debug("Uploaded chunk [" + response.getStatusCode() + "]: offset " + length + ", length " + cacheOffset);
		if (response.isSuccessStatusCode()) {
			response.getRequest().setParser(drive.getObjectParser());
			uploadedFile = response.parseAs(File.class);
		} else if (response.getStatusCode() != 308) {
			throw new IOException("Error while uploading chunk");
		}
		length += cacheOffset;
		cacheOffset = 0;
	}
	
	
	@SuppressWarnings("unused")
	private int getUploadStatus() throws IOException {
		HttpRequest request = drive.getRequestFactory().buildPutRequest(new GenericUrl(uploadUri), new EmptyContent());
		request.getHeaders().setContentRange("bytes */*");
		request.setThrowExceptionOnExecuteError(false);
		HttpResponse response = request.execute();
		
		if (response.isSuccessStatusCode() && uploadCache == null) {
			response.getRequest().setParser(drive.getObjectParser());
			uploadedFile = response.parseAs(File.class);
		} else if (response.getStatusCode() != 308) {
			throw new IOException("Unable to retrieve upload status");
		}
		return response.getStatusCode();
		//logger.info(response.getHeaders().getContentRange());
	}
	
}
