package org.andresoviedo.apps.gdrive_ftp_adapter.view.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.andresoviedo.apps.gdrive_ftp_adapter.controller.Controller;
import org.andresoviedo.apps.gdrive_ftp_adapter.model.Cache;
import org.andresoviedo.apps.gdrive_ftp_adapter.model.GoogleDrive.GFile;
import org.andresoviedo.util.Path;
import org.andresoviedo.util.os.OSUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.command.impl.MLSD;
import org.apache.ftpserver.command.impl.RETR;
import org.apache.ftpserver.command.impl.RNTO;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.AnonymousAuthentication;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.usermanager.UserManagerFactory;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.AbstractUserManager;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.ftpserver.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

public class GFtpServerFactory extends FtpServerFactory {

	private static final Log LOG = LogFactory.getLog(GFtpServerFactory.class);

	private final Controller controller;
	private final Cache model;
	private final Properties configuration;

	private static final String DEFAULT_ILLEGAL_CHARS_REGEX = "\\/|[\\x00-\\x1F\\x7F]|\\`|\\?|\\*|\\\\|\\<|\\>|\\||\\\"|\\:";
	private final Pattern illegalChars;

	public GFtpServerFactory(Controller controller, Cache model, Properties configuration) {
		super();
		this.controller = controller;
		this.model = model;
		this.configuration = configuration;
		this.illegalChars = Pattern.compile(configuration.getProperty("os.illegalCharacters", DEFAULT_ILLEGAL_CHARS_REGEX));
		LOG.info("Configured illegalchars '" + illegalChars + "'");
		init();
	}

	private void init() {
		setFileSystem(new FtpFileSystemView());
		ConnectionConfigFactory connectionConfigFactory = new ConnectionConfigFactory();
		connectionConfigFactory.setMaxThreads(10);
		connectionConfigFactory.setAnonymousLoginEnabled(true);
		setConnectionConfig(connectionConfigFactory.createConnectionConfig());
		setUserManager(new FtpUserManagerFactory().createUserManager());

		// MFMT for directories (default mina command doesn't support it)
		CommandFactoryFactory ccf = new CommandFactoryFactory();
		ccf.addCommand("MFMT", new FtpCommands.MFMT());
		setCommandFactory(ccf.createCommandFactory());

		// set the port of the listener
		int port = Integer.parseInt(configuration.getProperty("port", String.valueOf(1821)));
		LOG.info("FTP server configured at port '" + port + "'");
		ListenerFactory listenerFactory = new ListenerFactory();
		listenerFactory.setPort(port);

		// replace the default listener
		addListener("default", listenerFactory.createListener());

	}

	class FtpUserManagerFactory implements UserManagerFactory {

		@Override
		public UserManager createUserManager() {
			return new FtpUserManager("admin", new ClearTextPasswordEncryptor());
		}
	}

	class FtpUserManager extends AbstractUserManager {

		private BaseUser defaultUser;
		private BaseUser anonUser;

		public FtpUserManager(String adminName, PasswordEncryptor passwordEncryptor) {
			super(adminName, passwordEncryptor);

			defaultUser = new BaseUser();
			defaultUser.setAuthorities(Arrays.asList(new Authority[] { new ConcurrentLoginPermission(1, 1) }));
			defaultUser.setEnabled(true);
			defaultUser.setHomeDirectory("c:\\temp");
			defaultUser.setMaxIdleTime(300);
			defaultUser.setName(configuration.getProperty("ftp.user", "user"));
			defaultUser.setPassword(configuration.getProperty("ftp.pass", "user"));
			List<Authority> authorities = new ArrayList<Authority>();
			authorities.add(new WritePermission());
			authorities.add(new ConcurrentLoginPermission(10, 5));
			defaultUser.setAuthorities(authorities);
			LOG.info("FTP User Manager configured for user '" + defaultUser.getName() + "'");

			anonUser = new BaseUser(defaultUser);
			anonUser.setName("anonymous");
			anonUser.setEnabled(Boolean.valueOf(configuration.getProperty("ftp.anonymous.enabled", "false")));
		}

		@Override
		public User getUserByName(String username) throws FtpException {
			if (defaultUser.getName().equals(username)) {
				return defaultUser;
			} else if (anonUser.getName().equals(username)) {
				return anonUser;
			}

			return null;
		}

		@Override
		public String[] getAllUserNames() throws FtpException {
			return new String[] { defaultUser.getName(), anonUser.getName() };
		}

		@Override
		public void delete(String username) throws FtpException {
			// no opt
		}

		@Override
		public void save(User user) throws FtpException {
			// no opt
			LOG.info("save");
		}

		@Override
		public boolean doesExist(String username) throws FtpException {
			return ((defaultUser.getEnabled() && defaultUser.getName().equals(username)) || (anonUser.getEnabled() && anonUser.getName()
					.equals(username))) ? true : false;
		}

		@Override
		public User authenticate(Authentication authentication) throws AuthenticationFailedException {
			if (UsernamePasswordAuthentication.class.isAssignableFrom(authentication.getClass())) {
				UsernamePasswordAuthentication upAuth = (UsernamePasswordAuthentication) authentication;

				if (defaultUser.getEnabled() && defaultUser.getName().equals(upAuth.getUsername())
						&& defaultUser.getPassword().equals(upAuth.getPassword())) {
					return defaultUser;
				}

				if (anonUser.getEnabled() && anonUser.getName().equals(upAuth.getUsername())) {
					return anonUser;
				}
			} else if (AnonymousAuthentication.class.isAssignableFrom(authentication.getClass())) {
				return anonUser.getEnabled() ? anonUser : null;
			}

			return null;
		}
	}

	class FtpFileSystemView implements FileSystemFactory, FileSystemView {

		class FtpFileWrapper implements FtpFile {

			private final FtpFileWrapper parent;

			private final GFile gfile;

			/**
			 * This is not final because this name can change if there is other file in the same folder with the same name
			 */
			private String virtualName;

			public FtpFileWrapper(FtpFileWrapper parent, GFile ftpGFile, String virtualName) {
				this.parent = parent;
				this.gfile = ftpGFile;
				this.virtualName = virtualName;
			}

			public String getId() {
				return gfile.getId();
			}

			@Override
			public String getAbsolutePath() {
				/**
				 * This should handle the following 3 cases:
				 * <ul>
				 * <li>root = /</li>
				 * <li>root/file = /file</li>
				 * <li>root/folder/file = /folder/file</li>
				 * </ul>
				 */
				return isRoot() ? virtualName : parent.isRoot() ? FILE_SEPARATOR + virtualName : parent.getAbsolutePath() + FILE_SEPARATOR
						+ virtualName;
			}

			@Override
			public boolean isHidden() {
				// TODO: does google support hiding files?
				return false;
			}

			@Override
			public boolean isFile() {
				return !isDirectory();
			}

			@Override
			public boolean doesExist() {
				return gfile.isExists();
			}

			@Override
			public boolean isReadable() {
				// TODO: does google support read locking for files?
				return true;
			}

			@Override
			public boolean isWritable() {
				// TODO: does google support write locking of files?
				return true;
			}

			@Override
			public boolean isRemovable() {
				return gfile.isRemovable();
			}

			@Override
			public String getOwnerName() {
				return gfile.getOwnerName();
			}

			@Override
			public String getGroupName() {
				return "no_group";
			}

			@Override
			public int getLinkCount() {
				return gfile.getParents() != null ? gfile.getParents().size() : 0;
			}

			@Override
			public long getSize() {
				return gfile.getSize();
			}

			@Override
			public boolean delete() {
				if (!doesExist()) {
					throw new RuntimeException("Oops! Tried to delete file '" + getName() + "' although it doesn't exists");
				}
				return controller.trashFile(this.unwrap());
			}

			@Override
			public long getLastModified() {
				return gfile.getLastModified();
			}

			@Override
			public String getName() {
				return virtualName;
			}

			@Override
			public boolean isDirectory() {
				return gfile.isDirectory();
			}

			public GFile unwrap() {
				return gfile;
			}

			// ---------------- SETTERS ------------------ //

			@Override
			public boolean move(FtpFile destination) {
				return controller.renameFile(this.unwrap(), destination.getName());
			}

			@Override
			public OutputStream createOutputStream(long offset) throws IOException {
				return controller.createOutputStream(this.unwrap(), offset);
			}

			@Override
			public InputStream createInputStream(long offset) throws IOException {
				return controller.createInputStream(this.unwrap(), offset);
			}

			@Override
			public boolean mkdir() {
				if (isRoot()) {
					throw new IllegalArgumentException("Cannot create root folder");
				}
				return controller.mkdir(parent.getId(), this.unwrap());
			}

			@Override
			public boolean setLastModified(long arg0) {
				return controller.updateLastModified(this.unwrap(), arg0);
			}

			@Override
			public List<FtpFile> listFiles() {
				return FtpFileSystemView.this.listFiles(this);
			}

			@Override
			public String toString() {
				return "FtpFileWrapper [absolutePath=" + getAbsolutePath() + "]";
			}

			public boolean isRoot() {
				return parent == null;
			}

			public FtpFileWrapper getParentFile() {
				return parent;
			}

			public void setVirtualName(String virtualName) {
				this.virtualName = virtualName;
			}
		}

		public static final String DUP_FILE_TOKEN = "__ID__";

		// public static final String PEDING_SYNCHRONIZATION_TOKEN = "__UNSYNCH__";

		public static final String FILE_SEPARATOR = "/";

		public static final String FILE_PARENT = "..";

		public static final String FILE_SELF = ".";

		private final Pattern ENCODED_FILE_PATTERN = Pattern.compile("^(.*)\\Q" + DUP_FILE_TOKEN + "\\E(.{28})\\Q" + DUP_FILE_TOKEN
				+ "\\E(.*)$");

		private final User user;

		private FtpFileWrapper home;

		private FtpFileWrapper currentDir;

		public FtpFileSystemView() {
			this.user = null;
		}

		public FtpFileSystemView(User user) {
			this.user = user;
		}

		@Override
		public FileSystemView createFileSystemView(User user) throws FtpException {
			LOG.info("Creating ftp view for user '" + user + "'...");
			return new FtpFileSystemView(user);
		}

		@Override
		public boolean isRandomAccessible() throws FtpException {
			// TODO: true?
			return true;
		}

		@Override
		public FtpFile getHomeDirectory() throws FtpException {
			LOG.debug("Getting home directory for user '" + user + "'...");
			return home;
		}

		@Override
		public FtpFile getWorkingDirectory() throws FtpException {

			// initialize working directory in case this is the first call
			initWorkingDirectory();

			return currentDir;
		}

		private void initWorkingDirectory() {
			if (currentDir == null) {
				synchronized (this) {
					if (currentDir == null) {
						LOG.info("Initializing ftp view...");
						// TODO: what happen if a file is named "root"?
						this.home = new FtpFileWrapper(null, model.getFile("root"), "/");
						this.currentDir = this.home;
					}
				}
			}
		}

		@Override
		public boolean changeWorkingDirectory(String path) throws FtpException {
			try {
				// initialize working directory in case this is the first call
				initWorkingDirectory();
				
				LOG.debug("Changing working directory from '" + currentDir + "' to '" + path + "'...");

				// querying for home /
				if (Path.equals(FILE_SEPARATOR, path)) {
					currentDir = home;
					return true;
				}

				// changing to current dir
				if (Path.equals(FILE_SELF, path)) {
					return true;
				}

				// querying for parent ..
				if (Path.equals(FILE_PARENT, path)) {

					// lets get the parent for the current subfolder
					FtpFileWrapper parentFile = currentDir.getParentFile();
					if (parentFile == null) {
						// we are already in root directory
						return true;
					}

					// this is a deeper subfolder
					currentDir = currentDir.getParentFile();
					return true;
				}

				FtpFileWrapper file = null;
				LOG.debug("Changing working directory to path '" + path + "'...");
				file = getFileByPath(currentDir, path);

				if (file != null && file.isDirectory()) {
					currentDir = file;
					return true;
				}

				LOG.warn("File doesn't exist or is not a directory: '" + file + "'...");
				return false;
			} catch (Exception e) {
				throw new FtpException(e.getMessage(), e);
			}
		}

		@Override
		public void dispose() {
			currentDir = null;
		}

		/**
		 * This method is triggered when receiving a {@link MLSD} command or {@link RETR}.
		 * 
		 * The argument can be one of this:
		 * <ul>
		 * <li>"./": Should return the current directory (FileZilla tested!)</li>
		 * </ul>
		 */
		@Override
		public FtpFile getFile(String fileName) throws FtpException {

			// write log just for info
			StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
			StackTraceElement caller = stackTraceElements[2];
			if (RNTO.class.getName().equals(caller.getClassName()) && fileName.contains(DUP_FILE_TOKEN)) {
				LOG.info("User is renaming a file which contains special chars to this gdrive ftp adapter. Please avoid using the token '"
						+ DUP_FILE_TOKEN + "' in the filename.");
			}

			LOG.debug("Getting file '" + fileName + "'...");

			initWorkingDirectory();

			try {
				if (Path.equals("./", fileName)) {
					return currentDir;
				}

				if (fileName.length() == 0) {
					return currentDir;
				}

				return getFileByPath(currentDir, fileName);
				
			} catch (IllegalArgumentException e) {
				LOG.error(e.getMessage());
				throw new FtpException(e.getMessage(), e);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new FtpException(e.getMessage(), e);
			}
		}
		
		
		
		/**
		 * Get file by its path. The following paths are supported:
		 * <ul>
		 * <li>absolute path: /, /file.txt, /folder/file.txt</li>
		 * <li>relative to basefolder: file.txt, subfolder/file.txt</li>
		 * <li>relative to currentDir: file.txt, subfolder/file.txt</li>
		 * <ul>
		 * <li>basefolder = null</li>
		 * </ul>
		 * </ul>
		 * 
		 * Encoded file/folder names are supported:
		 * <ul>
		 * <li>/file__ID__google_file_id__ID__.txt</li>
		 * <li>/folder__ID__google_file_id__ID__/file.txt</li>
		 * <li>/folder__ID__google_file_id__ID__/subfolder__ID__google_file_id__ID__/file.txt</li>
		 * </ul>
		 * 
		 * Special paths like "." and ".." are NOT supported.
		 * 
		 * @param basefolder
		 *            the base folder, only used for relative paths
		 * @param fileName
		 *            the name of the file. This name can come encoded
		 * 
		 * @return the ftp wrapped file that can exist or not
		 */
		private FtpFileWrapper getFileByPath(FtpFileWrapper basefolder, String path) {
			if (Path.equals(currentDir.getAbsolutePath(), path)) {
				LOG.debug("Requested file is the current dir '" + currentDir + "'");
				return currentDir;
			}

			FtpFileWrapper folder;
			if (path.startsWith(Path.PATH_SEPARATOR)) {
				if (path.startsWith(Path.join(currentDir.getAbsolutePath(), Path.PATH_SEPARATOR))) {
					// relative to currentDir; get the relative filename
					folder = currentDir;
					path = path.substring(Path.join(currentDir.getAbsolutePath(), Path.PATH_SEPARATOR).length());
				} else {
					// absolute path; remove starting slash
					folder = home;
					path = path.substring(1);
				}
			} else {
				if (basefolder != null) {
					// relative path to basefolder
					folder = basefolder;
				} else {
					// relative path to currentDir
					folder = currentDir;
				}
			}
			
			LOG.debug("Getting file '" + path + "' inside directory '" + folder.getAbsolutePath() + "'...");
			for (String part : path.split(FtpFileSystemView.FILE_SEPARATOR)) {
				if (folder == null) {
					LOG.debug("file '" + path + "' not found !");
					return null;
				}
				folder = getFileByName(folder, part);
			}
			return folder;
		}
		

		/**
		 * Get file by its name. Name can be any of the following:
		 * 
		 * <ul>
		 * <li>file.txt</li>
		 * <li>file__ID__google_file_id__ID__.txt</li>
		 * </ul>
		 * 
		 * Folder can be any of the following:
		 * <ul>
		 * <li>absolute:</li>
		 * <ul>
		 * <li>/</li>
		 * <li>/folder</li>
		 * <li>/folder/subfolder/</li>
		 * <li>/folder__ID__google_file_id__ID__</li>
		 * <li>/folder__ID__google_file_id__ID__/subfolder__ID__google_file_id__ID__/</li>
		 * </ul>
		 * <li>relative to current directory:</li>
		 * <ul>
		 * <li>folder</li>
		 * <li>folder/subfolder/</li>
		 * <li>folder__ID__google_file_id__ID__</li>
		 * <li>folder__ID__google_file_id__ID__/subfolder__ID__google_file_id__ID__/</li>
		 * </ul>
		 * </ul>
		 * 
		 * @param folder
		 *            the folder containing the file
		 * @param fileName
		 *            the name of the file. This name can come encoded
		 * 
		 * @return the ftp wrapped file that can exist or not
		 */
		private FtpFileWrapper getFileByName(FtpFileWrapper folder, String fileName) {
			
			LOG.debug("Querying for file '" + fileName + "' inside folder '" + folder.getAbsolutePath() + "'...");
			
			try {
				String[] filenameDecodedInfo = decodeFilename(fileName);
				String decodedFilename = filenameDecodedInfo[0];
				String fileId = filenameDecodedInfo[1];
				
				GFile cachedFile;
				if (fileId != null) {
					LOG.info("Searching decoded file '" + Path.join(folder.getAbsolutePath(), decodedFilename) + "' ('" + fileId + "')...");
					cachedFile = model.getFile(fileId);
					if (cachedFile != null && removeIllegalChars(cachedFile.getName()).equals(decodedFilename)) {
						// && cachedFile.getParents().contains(folder.getId())
						// TODO: check for parents as soon as SQL cache supports it (rowmapper)
						// The file id exists, but we have to check also for equality of filename and containing folder
						// so we are quite sure the referred file is the same
						LOG.debug("Encoded file '" + fileName + "' found");
						return createFtpFileWrapper(folder, cachedFile, fileName, true);
					} else {
						LOG.info("Encoded file '" + fileName + "' ('" + fileId + "') not found");
					}

				} else {
					cachedFile = model.getFileByName(folder.getId(), fileName);
					if (cachedFile != null) {
						LOG.debug("File '" + fileName + "' found");
						return createFtpFileWrapper(folder, cachedFile, fileName, true);
					} else {
						LOG.debug("File '" + fileName + "' doesn't exist!");
					}
				}
				
				// file does not exist
				return createFtpFileWrapper(folder, new GFile(Collections.singleton(folder.getId()), fileName), fileName, false);
				
			} catch (IncorrectResultSizeDataAccessException e) {
				// INFO: this happens when the user wants to get a file which actually exists, but because it's
				// duplicated, the client should see the generated virtual name (filename encoded name with id).
				// INFO: in this case, we return a new file (although it exists), because virtually speaking the file
				// doesn't exists with that name
				return createFtpFileWrapper(folder, new GFile(Collections.singleton(folder.getId()), fileName), fileName, false);
			}
		}

		private FtpFileWrapper createFtpFileWrapper(FtpFileWrapper folder, GFile gFile, String filename, boolean exists) {

			// now lets remove illegal chars
			String filenameWithoutIllegalChars = removeIllegalChars(filename);
			if (!filename.equals(filenameWithoutIllegalChars)) {
				// update variable
				filename = encodeFilename(filenameWithoutIllegalChars, gFile.getId());
				LOG.info("Filename with illegal chars '" + filename + "' has been given virtual name '" + filenameWithoutIllegalChars + "'");
			}

			LOG.debug("Creating file wrapper " + (folder == null ? filename : Path.join(folder.getAbsolutePath(), filename)));
			return new FtpFileWrapper(folder, gFile, filename);
		}

		private String removeIllegalChars(String filename) {
			// now lets remove illegal chars
			return illegalChars.matcher(filename).replaceAll("_");
		}

		public List<FtpFile> listFiles(FtpFileWrapper folder) {

			LOG.info("Listing " + folder.getAbsolutePath());

			List<GFile> query = controller.getFiles(folder.getId());
			if (query.isEmpty()) {
				return Collections.<FtpFile> emptyList();
			}

			// list of all filenames found
			Map<String, FtpFileWrapper> allFilenames = new HashMap<String, FtpFileWrapper>(query.size());

			List<FtpFileWrapper> ret = new ArrayList<FtpFileWrapper>(query.size());

			// encode filenames if necessary (duplicated files, illegal chars, ...)
			for (GFile ftpFile : query) {

				FtpFileWrapper fileWrapper = createFtpFileWrapper(folder, ftpFile, ftpFile.getName(), true);
				ret.add(fileWrapper);

				// windows doesn't distinguish the case, unix does
				// windows & linux can't have repeated filenames
				// TODO: other OS I don't know yet...
				String filename = fileWrapper.getName();
				String uniqueFilename = OSUtils.isWindows() ? filename.toLowerCase() : OSUtils.isUnix() ? filename : filename;

				// check if the filename is not yet duplicated
				if (!allFilenames.containsKey(uniqueFilename)) {
					allFilenames.put(uniqueFilename, fileWrapper);
					continue;
				}

				// these are the repeated files
				final FtpFileWrapper firstFileDuplicated = allFilenames.get(uniqueFilename);
				firstFileDuplicated.setVirtualName(encodeFilename(filename, firstFileDuplicated.getId()));
				fileWrapper.setVirtualName(encodeFilename(filename, ftpFile.getId()));

				LOG.info("Generated virtual filename for duplicated file '" + firstFileDuplicated.getName() + "'");
				LOG.info("Generated virtual filename for duplicated file '" + fileWrapper.getName() + "'");
			}

			return new ArrayList<FtpFile>(ret);
		}

		private String encodeFilename(String filename, String fileId) {
			// split the file name & extension (if it applies) so we can inject the google file id within the two
			final int fileSuffixPos = filename.lastIndexOf('.');
			String ext = "";
			if (fileSuffixPos != -1) {
				ext = filename.substring(fileSuffixPos);
				filename = filename.substring(0, fileSuffixPos);
			}

			// lets change the filename so we simulate we have different filenames...
			// this instruction by the way is executed several times but it doesn't matter because the generated
			// name is always the same
			return filename + DUP_FILE_TOKEN + fileId + DUP_FILE_TOKEN + ext;
		}
		
		/**
		 * Decodes the filename (if encoded)
		 * @param filename name of a file without path
		 * @return String[] containing [0] decoded filename and [1] file id (if encoded) or null
		 */
		private String[] decodeFilename(String filename) {
			// Encoded?
			int nextIdx = filename.indexOf(DUP_FILE_TOKEN);
			if (nextIdx != -1 && ENCODED_FILE_PATTERN.matcher(filename).matches()) {
				// Get file when the name is encoded. The encode name has the form:
				// <filename>__ID__<google_file_id>_ID.<ext>
				Matcher matcher = ENCODED_FILE_PATTERN.matcher(filename);
				matcher.find();

				// Decode file name & id...
				return new String[] {matcher.group(1) + matcher.group(3), matcher.group(2)};
			}
			// not encoded
			return new String[] {filename, null};
		}
	}

	static class FtpCommands {
		public static class MFMT extends AbstractCommand {

			private final Logger LOG = LoggerFactory.getLogger(MFMT.class);

			/**
			 * Execute command.
			 */
			public void execute(final FtpIoSession session, final FtpServerContext context, final FtpRequest request) throws IOException {

				// reset state variables
				session.resetState();

				String argument = request.getArgument();

				if (argument == null || argument.trim().length() == 0) {
					session.write(LocalizedFtpReply.translate(session, request, context,
							FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS, "MFMT.invalid", null));
					return;
				}

				String[] arguments = argument.split(" ", 2);

				if (arguments.length != 2) {
					session.write(LocalizedFtpReply.translate(session, request, context,
							FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS, "MFMT.invalid", null));
					return;
				}

				String timestamp = arguments[0].trim();

				try {

					Date time = DateUtils.parseFTPDate(timestamp);

					String fileName = arguments[1].trim();

					// get file object
					FtpFile file = null;

					try {
						file = session.getFileSystemView().getFile(fileName);
					} catch (Exception ex) {
						LOG.debug("Exception getting the file object: " + fileName, ex);
					}

					if (file == null || !file.doesExist()) {
						session.write(LocalizedFtpReply.translate(session, request, context, FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
								"MFMT.filemissing", fileName));
						return;
					}

					// INFO: We want folders also to be touched
					// // check file
					// if (!file.isFile()) {
					// session.write(LocalizedFtpReply
					// .translate(
					// session,
					// request,
					// context,
					// FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
					// "MFMT.invalid", null));
					// return;
					// }

					// check if we can set date and retrieve the actual date
					// stored
					// for the file.
					if (!file.setLastModified(time.getTime())) {
						// we couldn't set the date, possibly the file was
						// locked
						session.write(LocalizedFtpReply.translate(session, request, context,
								FtpReply.REPLY_450_REQUESTED_FILE_ACTION_NOT_TAKEN, "MFMT", fileName));
						return;
					}

					// all checks okay, lets go
					session.write(LocalizedFtpReply.translate(session, request, context, FtpReply.REPLY_213_FILE_STATUS, "MFMT",
							"ModifyTime=" + timestamp + "; " + fileName));
					return;

				} catch (ParseException e) {
					session.write(LocalizedFtpReply.translate(session, request, context,
							FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS, "MFMT.invalid", null));
					return;
				}

			}
		}
	}

}
