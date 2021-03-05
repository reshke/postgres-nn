/*-------------------------------------------------------------------------
 *
 * memstore.c
 *	  Base backup + WAL in a memstore file format
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/memstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <netinet/tcp.h>
#include <errno.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/memstore.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "access/xlog_internal.h"


static pgsocket msSock;

static bool
SetSocketOptions(pgsocket sock)
{
	int on = 1;
	if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
				   (char *) &on, sizeof(on)) < 0)
	{
		elog(FATAL, "setsockopt(%s) failed: %m", "TCP_NODELAY");
		closesocket(sock);
		return false;
	}
	return true;
}

static pgsocket
ConnectSocket(char const* host, char const* port)
{
	struct addrinfo *addrs = NULL,
		*addr,
		hints;
	int	ret;
	pgsocket sock = PGINVALID_SOCKET;

	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_addr = NULL;
	hints.ai_canonname = NULL;
	hints.ai_next = NULL;
	ret = getaddrinfo(host, port, &hints, &addrs);
	if (ret || !addrs)
	{
		elog(FATAL, "Could not resolve \"%s\": %m",	 host);
		return -1;
	}
	for (addr = addrs; addr; addr = addr->ai_next)
	{
		sock = socket(addr->ai_family, SOCK_STREAM, 0);
		if (sock == PGINVALID_SOCKET)
		{
			elog(FATAL, "could not create socket: %m");
			continue;
		}
		if (!SetSocketOptions(sock))
			continue;

		/*
		 * Bind it to a kernel assigned port on localhost and get the assigned
		 * port via getsockname().
		 */
		while ((ret = connect(sock, addr->ai_addr, addr->ai_addrlen)) < 0 && errno == EINTR);
		if (ret < 0)
		{
			elog(FATAL, "Could not establish connection to %s:%s: %m",
				 host, port);
			closesocket(sock);
		}
		else
		{
			break;
		}
	}
	return sock;
}

static void
SendRequest(MemstoreRequest* req)
{
	if (write(msSock, req, sizeof(*req)) != sizeof(*req))
		elog(ERROR, "Failed to send request to memstore");
}

static void
SendData(void* data, size_t size)
{
	if (write(msSock, data, size) != size)
		elog(ERROR, "Failed to send datat to memstore");
}

static void
ReadResponse(void* data, size_t size)
{
	if (read(msSock, data, size) != size)
		elog(ERROR, "Failed to receive response from memstore");
}


/*
 *	memstore_init() -- Initialize private state
 */
void
memstore_init(void)
{
	msSock = ConnectSocket(MEMSTORE_HOST, MEMSTORE_PORT);
}

/*
 *	memstore_exists() -- Does the physical file exist?
 */
bool
memstore_exists(SMgrRelation reln, ForkNumber forkNum)
{
	MemstoreRequest req;
	bool exists;
	req.cmd = MS_EXISTS;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forkNum;
	SendRequest(&req);
	ReadResponse(&exists, sizeof(exists));
	return exists;
}

/*
 *	memstore_create() -- Create a new relation on memstored storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
memstore_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	MemstoreRequest req;
	req.cmd = MS_CREATE;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forkNum;
	SendRequest(&req);
}

/*
 *	memstore_unlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
memstore_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	MemstoreRequest req;
	req.cmd = MS_UNLINK;
	req.ref.file.rnode = rnode;
	req.ref.file.fork = forkNum;
	SendRequest(&req);
}

/*
 *	memstore_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
memstore_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				char *buffer, bool skipFsync)
{
	MemstoreRequest req;
	bool ok;;
	req.cmd = MS_WRITE;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forknum;
	req.ref.blkno = blocknum;
	SendRequest(&req);
	SendData(buffer, BLCKSZ);
	ReadResponse(&ok, sizeof ok);
	if (!ok)
		elog(ERROR, "Failed to write data");
}

/*
 *  memstore_open() -- Initialize newly-opened relation.
 */
void
memstore_open(SMgrRelation reln)
{
	/* no work */
}

/*
 *	memstore_close() -- Close the specified relation, if it isn't closed already.
 */
void
memstore_close(SMgrRelation reln, ForkNumber forknum)
{
	/* no work */
}

/*
 *	memstore_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
memstore_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	/* not implemented */
	return true;
}

/*
 * memstore_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
memstore_writeback(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, BlockNumber nblocks)
{
	/* not implemented */
}

/*
 *	memstore_read() -- Read the specified block from a relation.
 */
void
memstore_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				 char *buffer)
{
	MemstoreRequest req;
	req.cmd = MS_READ;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forknum;
	req.ref.blkno = blocknum;
	SendRequest(&req);
	ReadResponse(buffer, BLCKSZ);
}

/*
 *	memstore_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
memstore_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
	MemstoreRequest req;
	bool ok;
	req.cmd = MS_WRITE;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forknum;
	req.ref.blkno = blocknum;
	SendRequest(&req);
	SendData(buffer, BLCKSZ);
	ReadResponse(&ok, sizeof ok);
	if (!ok)
		elog(ERROR, "Failed to write data");
}

/*
 *	memstore_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
memstore_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	MemstoreRequest req;
	BlockNumber size;
	req.cmd = MS_SIZE;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forknum;
	SendRequest(&req);
	ReadResponse(&size, sizeof(size));
	return size;
}

/*
 *	memstore_truncate() -- Truncate relation to specified number of blocks.
 */
void
memstore_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	MemstoreRequest req;
	req.cmd = MS_TRUNC;
	req.ref.file.rnode = reln->smgr_rnode;
	req.ref.file.fork = forknum;
	req.ref.blkno = nblocks;
	SendRequest(&req);
}

/*
 *	memstore_immedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
void
memstore_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	/* FIXME: do nothing, we rely on WAL */
}

