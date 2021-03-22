/*-------------------------------------------------------------------------
 *
 * memstore.c -- simplistic in-memory page storage.
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/memstore.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlogutils.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "fmgr.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/proc.h"
#include "storage/pagestore_client.h"
#include "miscadmin.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"

#include "libpq/libpq.h"
#include "libpq/pqformat.h"

#include "memstore.h"
#include "zenith_store.h"

#include "access/xlog_internal.h"
#include "access/clog.h"
#include "access/xact.h"
#include "access/multixact.h"
#include "access/slru.h"
#include "catalog/pg_control.h"
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include "storage/copydir.h"

MemStore *memStore;

static MemoryContext RestoreCxt;

PG_FUNCTION_INFO_V1(zenith_store_get_page);
PG_FUNCTION_INFO_V1(zenith_store_dispatcher);
PG_FUNCTION_INFO_V1(zenith_store_init_computenode);

void
memstore_init()
{
	Size		size = 0;
	size = add_size(size, 1000*1000*1000);
	size = MAXALIGN(size);
	RequestAddinShmemSpace(size);

	RequestNamedLWLockTranche("memStoreLock", 1);
}

void
memstore_init_shmem()
{
	HASHCTL		info;
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	memStore = ShmemInitStruct("memStore",
								 sizeof(MemStore),
								 &found);

	if (!found)
		memStore->lock = &(GetNamedLWLockTranche("memStoreLock"))->lock;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(PerPageWalHashKey);
	info.entrysize = sizeof(PerPageWalHashEntry);
	memStore->pages = ShmemInitHash("memStorePages",
		10*1000, /* minsize */
		20*1000, /* maxsize */
		&info, HASH_ELEM | HASH_BLOBS);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(RelsHashKey);
	info.entrysize = sizeof(RelsHashEntry);
	memStore->rels = ShmemInitHash("memStoreRels",
		100, /* minsize */
		2000, /* maxsize */
		&info, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}


static int
pg_mkdir_recursive(char *path, int omode)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			last,
				retval;
	char	   *p;

	retval = 0;
	p = path;

	/*
	 * POSIX 1003.2: For each dir operand that does not name an existing
	 * directory, effects equivalent to those caused by the following command
	 * shall occur:
	 *
	 * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
	 *
	 * We change the user's umask and then restore it, instead of doing
	 * chmod's.  Note we assume umask() can't change errno.
	 */
	oumask = umask(0);
	numask = oumask & ~(S_IWUSR | S_IXUSR);
	(void) umask(numask);

	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;

		if (last)
			(void) umask(oumask);

		/* check for pre-existing directory */
		if (stat(path, &sb) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = -1;
				break;
			}
		}
		else if (mkdir(path, last ? omode : S_IRWXU | S_IRWXG | S_IRWXO) < 0)
		{
			retval = -1;
			break;
		}
		if (!last)
			*p = '/';
	}

	/* ensure we restored umask */
	(void) umask(oumask);

	return retval;
}

/* Given ConrtolFile and PGDATA path write fake Xlog,
 * that contains only one checkpoint record needed to start postgres.
 *
 * FIXME: Now it only works if checkpoint record is the first record in the segment
 */
static void
WriteFakeXLOG(char *base_path, ControlFileData ControlFile)
{
	PGAlignedXLogBlock buffer;
	PGAlignedXLogBlock targetbuffer;
	XLogPageHeader page;
	XLogLongPageHeader longpage;
	XLogRecord *record;
	pg_crc32c	crc;
	char xlog_relative_path[MAXPGPATH];
	char		*path;
	int			fd;
	int			nbytes;
	char	   *recptr;
	int newXlogSegNo;
	int newXlogPageNo;
	uint64 targetPageOff = XLogSegmentOffset(ControlFile.checkPointCopy.redo, ControlFile.xlog_seg_size);
	uint64 targetRecOff =  ControlFile.checkPointCopy.redo % XLOG_BLCKSZ;

	memset(buffer.data, 0, XLOG_BLCKSZ);
	memset(targetbuffer.data, 0, XLOG_BLCKSZ);

	newXlogSegNo = ControlFile.checkPointCopy.redo / ControlFile.xlog_seg_size;
	newXlogPageNo = targetPageOff / XLOG_BLCKSZ;

	elog(DEBUG5, "LSN %X/%X Segno %d blkno %d",
	(uint32) (ControlFile.checkPointCopy.redo >> 32),
	(uint32) ControlFile.checkPointCopy.redo,
	 newXlogSegNo, newXlogPageNo);

	/* Set up the target XLOG page */
	page = (XLogPageHeader) targetbuffer.data;
	page->xlp_magic = XLOG_PAGE_MAGIC;
	page->xlp_tli = ControlFile.checkPointCopy.ThisTimeLineID;

	if (newXlogSegNo == 0)
	{
		page->xlp_info = XLP_LONG_HEADER;
		page->xlp_pageaddr = ControlFile.checkPointCopy.redo - SizeOfXLogLongPHD;
		longpage = (XLogLongPageHeader) page;
		longpage->xlp_sysid = ControlFile.system_identifier;
		longpage->xlp_seg_size = ControlFile.xlog_seg_size;
		longpage->xlp_xlog_blcksz = ControlFile.xlog_blcksz;
	}
	else
		page->xlp_pageaddr = ControlFile.checkPointCopy.redo - SizeOfXLogShortPHD;	

	/* Insert the initial checkpoint record */
	recptr = (char *) page + SizeOfXLogLongPHD;
	record = (XLogRecord *) recptr;
	record->xl_prev = 0;
	record->xl_xid = InvalidTransactionId;
	record->xl_tot_len = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint);
	record->xl_info = XLOG_CHECKPOINT_SHUTDOWN;
	record->xl_rmid = RM_XLOG_ID;

	recptr += SizeOfXLogRecord;
	*(recptr++) = (char) XLR_BLOCK_ID_DATA_SHORT;
	*(recptr++) = sizeof(CheckPoint);
	memcpy(recptr, &ControlFile.checkPointCopy,
		   sizeof(CheckPoint));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	record->xl_crc = crc;

	XLogFilePath(xlog_relative_path, ControlFile.checkPointCopy.ThisTimeLineID,
				 newXlogSegNo, ControlFile.xlog_seg_size);

	path = psprintf("%s/%s", base_path, xlog_relative_path);
	fd = open(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd < 0)
	{
		zenith_log(ERROR,"could not open file \"%s\": %m", path);
		exit(1);
	}

	errno = 0;

	/* Fill the rest of the file with zeroes */
	for (nbytes = 0; nbytes < ControlFile.xlog_seg_size; nbytes += XLOG_BLCKSZ)
	{
		errno = 0;
		/* Write the block with generated checkpoint record. */
		if (nbytes / XLOG_BLCKSZ == newXlogSegNo)
		{
			elog(LOG, "write target page, %d", nbytes / XLOG_BLCKSZ);
			if(write(fd, targetbuffer.data, XLOG_BLCKSZ) != XLOG_BLCKSZ)
			{
				if (errno == 0)
					errno = ENOSPC;
				zenith_log(ERROR,"could not write file \"%s\": %m", path);
				exit(1);
			}
		}
		/* Write zeroed blocks. */
		else 
		{
			if (write(fd, buffer.data, XLOG_BLCKSZ) != XLOG_BLCKSZ)
			{
				if (errno == 0)
					errno = ENOSPC;
				zenith_log(ERROR,"could not write file \"%s\": %m", path);
				exit(1);
			}
		}
	}

	if (fsync(fd) != 0)
	{
		zenith_log(ERROR,"fsync error: %m");
		exit(1);
	}

	close(fd);
}

/* Given base path of compute node's PGDATA 
 *
 * write to this PGDATA new pg_control, fake WAL and few more files,
 * needed to start postgres
 */
Datum
zenith_store_init_computenode(PG_FUNCTION_ARGS)
{
	char *base_path = PG_GETARG_CSTRING(0);
	char *dir;
	char *fpath;
	bool crc_ok;
	ControlFileData *controlfile;
	FILE *f;

	if (!is_absolute_path(base_path))
		zenith_log(ERROR, "base_path must be absolute path");

	dir = psprintf("%s/global", base_path);
	pg_mkdir_recursive(dir, S_IRUSR | S_IWUSR);

	fpath = psprintf("%s/%s", base_path, XLOG_CONTROL_FILE);

	controlfile = get_controlfile(".", &crc_ok);

	controlfile->state = DB_SHUTDOWNED;

	//TODO use real LSN
	controlfile->checkPointCopy.redo = SizeOfXLogLongPHD;
	controlfile->checkPoint = controlfile->checkPointCopy.redo;

  	f = fopen (fpath,"w");
	fclose(f);

	update_controlfile(base_path, controlfile, true);

	/* generate mock WAL file */
	WriteFakeXLOG(base_path, *controlfile);

	/* copy relmapper files*/
	char *from_fpath = psprintf("global/pg_filenode.map");
	char *to_fpath = psprintf("%s/global/pg_filenode.map", base_path);

	copy_file(from_fpath, to_fpath);

	/* handle per database relmapper files
	 * TODO: Now it only works with template and current database
	 */

	dir = psprintf("%s/base/%u", base_path, MyDatabaseId);
	pg_mkdir_recursive(dir, S_IRWXU);

	from_fpath = psprintf("base/%u/pg_filenode.map", MyDatabaseId);
	to_fpath = psprintf("%s/base/%u/pg_filenode.map", base_path, MyDatabaseId);

	copy_file(from_fpath, to_fpath);

	/* Postgres wants to see PG_VERSION file in each database subdir. Copy it from template1
	 * TODO: Now it only works with template and current database
	 */
	from_fpath = psprintf("%s/base/1/PG_VERSION", base_path);
	to_fpath = psprintf("%s/base/%u/PG_VERSION", base_path, MyDatabaseId);

	copy_file(from_fpath, to_fpath);

	PG_RETURN_VOID();
}


void
memstore_insert(PerPageWalHashKey key, XLogRecPtr lsn, uint8 my_block_id, XLogRecord *record)
{
	bool found;
	PerPageWalHashEntry *hash_entry;
	PerPageWalRecord *list_entry;

	/*
	 * XXX: could be done with one allocation, but that is prototype anyway so
	 * don't bother with offsetoff.
	 */
	list_entry = ShmemAlloc(sizeof(PerPageWalRecord));
	list_entry->record = ShmemAlloc(record->xl_tot_len);
	list_entry->lsn = lsn;
	list_entry->prev = NULL;
	list_entry->next = NULL;
	list_entry->my_block_id = my_block_id;
	memcpy(list_entry->record, record, record->xl_tot_len);

	zenith_log(PageInsertTrace, "memstore_insert: \"%d.%d.%d.%d.%u\"",
		key.rnode.spcNode,
		key.rnode.dbNode,
		key.rnode.relNode,
		key.forknum,
		key.blkno
	);

	LWLockAcquire(memStore->lock, LW_EXCLUSIVE);

	hash_entry = (PerPageWalHashEntry *) hash_search(memStore->pages, &key, HASH_ENTER, &found);

	if (!found)
	{
		Assert(!hash_entry->newest);
		Assert(!hash_entry->oldest);

		bool rfound;
		RelsHashKey rkey;
		memset(&rkey, '\0', sizeof(RelsHashKey));
		rkey.system_identifier = key.system_identifier;
		rkey.forknum = key.forknum;
		rkey.rnode = key.rnode;

		RelsHashEntry *rel_entry = hash_search(memStore->rels, &rkey, HASH_ENTER, &rfound);
		if (!rfound)
			rel_entry->n_pages = 0;

		if (key.blkno >= rel_entry->n_pages)
		{
			rel_entry->n_pages = key.blkno + 1;
		}
	}

	PerPageWalRecord *prev_newest = hash_entry->newest;
	hash_entry->newest = list_entry;
	list_entry->prev = prev_newest;
	if (prev_newest)
		prev_newest->next = list_entry;
	if (!hash_entry->oldest)
		hash_entry->oldest = list_entry;

	LWLockRelease(memStore->lock);
}


PerPageWalRecord *
memstore_get_oldest(PerPageWalHashKey *key)
{
	bool found;
	PerPageWalHashEntry *hash_entry;
	PerPageWalRecord *result = NULL;

	LWLockAcquire(memStore->lock, LW_SHARED);

	hash_entry = (PerPageWalHashEntry *) hash_search(memStore->pages, key, HASH_FIND, &found);

	if (found) {
		result = hash_entry->oldest;
	}

	LWLockRelease(memStore->lock);

	/*
	 * For now we never modify records or change their next field, so unlocked
	 * access would be okay.
	 */
	return result;
}

uint8 current_block_id;
static bool
pagestore_redo_read_buffer_filter(XLogReaderState *reader_state, uint8 block_id)
{
	if (block_id == current_block_id)
	{
		return false; /* don't skip */
	}
	else
	{
		zenith_log(RequestTrace, "Redo: skip redo for block#%d, current is #%d",
			block_id, current_block_id);
		return true; /* skip */
	}
}

static void
get_page_by_key(PerPageWalHashKey *key, char **raw_page_data)
{
	PerPageWalRecord *record_entry = memstore_get_oldest(key);

	PerPageWalRecord *first_record_entry = record_entry;

	if (!record_entry)
	{
		memset(*raw_page_data, '\0', BLCKSZ);
		return;
	}

	/*
	 * TODO: properly hold buffer lock.
	 *
	 * Concurrent getPage requests spoils our pages.
	 */
	LWLockAcquire(memStore->lock, LW_EXCLUSIVE);

	/* recovery here */
	int			chain_len = 0;
	char	   *errormsg;
	for (; record_entry; record_entry = record_entry->next)
	{
		XLogReaderState reader_state = {
			.ReadRecPtr = 0,
			.EndRecPtr = record_entry->lsn,
			.decoded_record = record_entry->record
		};
		bool normal_redo = false;

		if (!DecodeXLogRecord(&reader_state, record_entry->record, &errormsg))
			zenith_log(ERROR, "failed to decode WAL record: %s", errormsg);

		/* use custom functions to avoid updating in memory Xid values */
		if (record_entry->record->xl_rmid == RM_XACT_ID)
		{
			uint8		info = record_entry->record->xl_info & XLOG_XACT_OPMASK;
			zenith_log(RequestTrace, "Handle RM_XACT_ID  record");
			if (info == XLOG_XACT_COMMIT)
				xact_redo_commit_pageserver(&reader_state);
			else if (info == XLOG_XACT_ABORT)
				xact_redo_abort_pageserver(&reader_state);
		}
		else if (record_entry->record->xl_rmid == RM_MULTIXACT_ID)
		{
			uint8		info = record_entry->record->xl_info & ~XLR_INFO_MASK;
			zenith_log(RequestTrace, "Handle RM_MULTIXACT_ID record");

			if (info == XLOG_MULTIXACT_CREATE_ID)
				multixact_redo_create_id_pageserver(&reader_state);
			else
				normal_redo = true;
		}

		if (normal_redo)
		{
			current_block_id = record_entry->my_block_id;
			InRecovery = true;
			RmgrTable[record_entry->record->xl_rmid].rm_redo(&reader_state);
			InRecovery = false;
		}

		chain_len++;
	}
	zenith_log(RequestTrace, "Page restored: chain len is %d, last entry ptr=%p",
		chain_len, first_record_entry);

	if (key->rnode.relNode == CLOG_RELNODE || 
		key->rnode.relNode == MULTIXACT_OFFSETS_RELNODE ||
		key->rnode.relNode == MULTIXACT_MEMBERS_RELNODE)
	{
		zenith_log(RequestTrace, "Page restored SLRU: chain len is %d, last entry ptr=%p",
				   chain_len, first_record_entry);

		if (key->rnode.relNode == CLOG_RELNODE)
			get_xact_page_copy(key->blkno, *raw_page_data);
		else if (key->rnode.relNode == MULTIXACT_OFFSETS_RELNODE)
			get_multixact_offset_page_copy(key->blkno, *raw_page_data);
		else if (key->rnode.relNode == MULTIXACT_MEMBERS_RELNODE)
			get_multixact_member_page_copy(key->blkno, *raw_page_data);

		return;
	}

	/* Take a verbatim copy of the page in shared buffers */
	Buffer		buf;
	BufferTag	newTag;			/* identity of requested block */
	uint32		newHash;		/* hash value for newTag */
	LWLock	   *newPartitionLock;	/* buffer partition lock for it */
	int			buf_id;
	bool		valid = false;
	BufferDesc *bufdesc = NULL;

	/* create a tag so we can lookup the buffer */
	INIT_BUFFERTAG(newTag, key->rnode, key->forknum, key->blkno);

	/* determine its hash code and partition lock ID */
	newHash = BufTableHashCode(&newTag);
	newPartitionLock = BufMappingPartitionLock(newHash);

	/* see if the block is in the buffer pool already */
	LWLockAcquire(newPartitionLock, LW_SHARED);
	buf_id = BufTableLookup(&newTag, newHash);

	if (buf_id >= 0)
	{
		bufdesc = GetBufferDescriptor(buf_id);
		valid = PinBuffer(bufdesc, NULL);
	}
	/* Can release the mapping lock as soon as we've pinned it */
	LWLockRelease(newPartitionLock);

	if (!valid)
		zenith_log(ERROR, "Can't pin buffer");

	buf = BufferDescriptorGetBuffer(bufdesc);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	memcpy(*raw_page_data, BufferGetPage(buf), BLCKSZ);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);

	BufferDesc *bufHdr;
	uint32		buf_state;
	bufHdr = GetBufferDescriptor(buf_id);
	buf_state = LockBufHdr(bufHdr);

	if (RelFileNodeEquals(bufHdr->tag.rnode, key->rnode) &&
		bufHdr->tag.forkNum == key->forknum &&
		bufHdr->tag.blockNum == key->blkno)
		InvalidateBuffer(bufHdr);	/* releases spinlock */
	else
		UnlockBufHdr(bufHdr, buf_state);

	LWLockRelease(memStore->lock);
}

Datum
zenith_store_get_page(PG_FUNCTION_ARGS)
{
	uint64		sysid = PG_GETARG_INT64(0);
	Oid			tspaceno = PG_GETARG_OID(1);
	Oid			dbno = PG_GETARG_OID(2);
	Oid			relno = PG_GETARG_OID(3);
	ForkNumber	forknum = PG_GETARG_INT32(4);
	int64		blkno = PG_GETARG_INT64(5);

	bytea	   *raw_page;
	char	   *raw_page_data;

	RestoreCxt = AllocSetContextCreate(TopMemoryContext,
										   "RestoreSmgr",
										   ALLOCSET_DEFAULT_SIZES);

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	PerPageWalHashKey key;
	memset(&key, '\0', sizeof(PerPageWalHashKey));
	key.system_identifier = sysid;
	key.rnode.spcNode = tspaceno;
	key.rnode.dbNode = dbno;
	key.rnode.relNode = relno;
	key.forknum = forknum;
	key.blkno = blkno;

	get_page_by_key(&key, &raw_page_data);

	PG_RETURN_BYTEA_P(raw_page);
}

Datum
zenith_store_dispatcher(PG_FUNCTION_ARGS)
{
	StringInfoData s;
	int			rmid;

	/* switch client to COPYBOTH */
	pq_beginmessage(&s, 'W');
	pq_sendbyte(&s, 0);			/* copy_is_binary */
	pq_sendint16(&s, 0);		/* numAttributes */
	pq_endmessage(&s);
	pq_flush();

	redo_read_buffer_filter = pagestore_redo_read_buffer_filter;

	zenith_log(RequestTrace, "got connection");

	/* Initialize resource managers */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}

	for (;;)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ModifyWaitEvent(FeBeWaitSet, 0, WL_SOCKET_READABLE, NULL);

		pq_startmsgread();
		pq_getbyte(); /* libpq message type 'd' */
		if (pq_getmessage(&msg, 0) != 0)
			zenith_log(ERROR, "failed to read client request");

		ZenithMessage *raw_req = zm_unpack(&msg);

		zenith_log(RequestTrace, "got page request: %s", zm_to_string(raw_req));

		if (messageTag(raw_req) != T_ZenithExistsRequest
			&& messageTag(raw_req) != T_ZenithTruncRequest
			&& messageTag(raw_req) != T_ZenithUnlinkRequest
			&& messageTag(raw_req) != T_ZenithNblocksRequest
			&& messageTag(raw_req) != T_ZenithReadRequest
			&& messageTag(raw_req) != T_ZenithPageExistsRequest
			&& messageTag(raw_req) != T_ZenithCreateRequest
			&& messageTag(raw_req) != T_ZenithExtendRequest)
		{
			zenith_log(ERROR, "invalid request tag %d", messageTag(raw_req));
		}
		ZenithRequest *req = (ZenithRequest *) raw_req;

		ZenithResponse resp;
		memset(&resp, '\0', sizeof(ZenithResponse));

		if (req->tag == T_ZenithExistsRequest)
		{
			bool found;
			RelsHashKey rkey;
			memset(&rkey, '\0', sizeof(RelsHashKey));
			rkey.system_identifier = req->system_id;
			rkey.forknum = req->page_key.forknum;
			rkey.rnode = req->page_key.rnode;

			LWLockAcquire(memStore->lock, LW_SHARED);
			hash_search(memStore->rels, &rkey, HASH_FIND, &found);
			LWLockRelease(memStore->lock);

			resp.tag = T_ZenithStatusResponse;
			resp.ok = found;
		}
		else if (req->tag == T_ZenithTruncRequest)
		{
			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithUnlinkRequest)
		{
			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithCreateRequest)
		{
			bool found;
			RelsHashKey rkey;
			RelsHashEntry *rentry;
			memset(&rkey, '\0', sizeof(RelsHashKey));
			rkey.system_identifier = req->system_id;
			rkey.forknum = req->page_key.forknum;
			rkey.rnode = req->page_key.rnode;

			LWLockAcquire(memStore->lock, LW_EXCLUSIVE);
			rentry = (RelsHashEntry *) hash_search(memStore->rels, &rkey, HASH_ENTER, &found);
			rentry->n_pages = 0;
			LWLockRelease(memStore->lock);

			Assert(!found);
			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithNblocksRequest)
		{
			bool found;
			uint32 n_pages;
			RelsHashKey rkey;
			RelsHashEntry *rentry;
			memset(&rkey, '\0', sizeof(RelsHashKey));
			rkey.system_identifier = req->system_id;
			rkey.forknum = req->page_key.forknum;
			rkey.rnode = req->page_key.rnode;

			LWLockAcquire(memStore->lock, LW_EXCLUSIVE);
			rentry = (RelsHashEntry *) hash_search(memStore->rels, &rkey, HASH_ENTER, &found);
			if (!found)
				rentry->n_pages = 0;
			n_pages = rentry->n_pages;
			LWLockRelease(memStore->lock);

			resp.tag = T_ZenithNblocksResponse;
			resp.n_blocks = n_pages;
			resp.ok = true;

			elog(LOG, "nblocks: %d (found: %d)", n_pages, found);
		}
		else if (req->tag == T_ZenithPageExistsRequest)
		{
			PerPageWalHashKey key;
			char *raw_page_data = (char *) palloc(BLCKSZ);
			bool found = false;
			PerPageWalHashEntry *entry;

			memset(&key, '\0', sizeof(PerPageWalHashKey));
			key.system_identifier = req->system_id;
			key.rnode = req->page_key.rnode;
			key.forknum = req->page_key.forknum;
			key.blkno = req->page_key.blkno;

			LWLockAcquire(memStore->lock, LW_EXCLUSIVE);
			entry = (PerPageWalHashEntry *) hash_search(memStore->pages, &key, HASH_FIND, &found);
			resp.tag = T_ZenithStatusResponse;
			resp.ok = found;
			LWLockRelease(memStore->lock);

		}
		else if (req->tag == T_ZenithReadRequest)
		{
			PerPageWalHashKey key;
			char *raw_page_data = (char *) palloc(BLCKSZ);

			memset(&key, '\0', sizeof(PerPageWalHashKey));
			key.system_identifier = req->system_id;
			key.rnode = req->page_key.rnode;
			key.forknum = req->page_key.forknum;
			key.blkno = req->page_key.blkno;

			get_page_by_key(&key, &raw_page_data);

			resp.tag = T_ZenithReadResponse;
			resp.page = raw_page_data;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithExtendRequest)
		{
			bool found;
			RelsHashKey rkey;
			RelsHashEntry *rentry;
			memset(&rkey, '\0', sizeof(RelsHashKey));
			rkey.system_identifier = req->system_id;
			rkey.forknum = req->page_key.forknum;
			rkey.rnode = req->page_key.rnode;

			LWLockAcquire(memStore->lock, LW_EXCLUSIVE);
			rentry = (RelsHashEntry *) hash_search(memStore->rels, &rkey, HASH_ENTER, &found);
			if (!found)
				rentry->n_pages = 0;
			if (req->page_key.blkno >= rentry->n_pages)
				rentry->n_pages = req->page_key.blkno + 1;
			LWLockRelease(memStore->lock);

			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else
			Assert(false);

		/* respond */
		StringInfoData resp_str = zm_pack((ZenithMessage *) &resp);
		resp_str.cursor = 'd';
		pq_endmessage(&resp_str);
		pq_flush();

		zenith_log(RequestTrace, "responded: %s", ZenithMessageStr[raw_req->tag]);

		CHECK_FOR_INTERRUPTS();

		// XXX: clear memory context
	}
}

