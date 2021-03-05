/*-------------------------------------------------------------------------
 *
 * memstore.h
 *	  public interface declarations for restoring data files lazily.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/memstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMSTORE_H
#define MEMSTORE_H

#define MEMSTORE_HOST "localhost"
#define MEMSTORE_PORT "5445"


typedef enum
{
	MS_EXISTS,
	MS_CREATE,
	MS_READ,
	MS_WRITE,
	MS_SIZE,
	MS_TRUNC,
	MS_UNLINK
} MemstorCommand;

typedef struct FileRef
{
	RelFileNodeBackend rnode;
	ForkNumber         fork;
} FileRef;

typedef struct BlockRef
{
	FileRef     file;
	BlockNumber blkno;
} BlockRef;

typedef struct MemstoreRequest
{
	MemstorCommand cmd;
	BlockRef       ref;
} MemstoreRequest;

/* memstore storage manager functionality */
extern void memstore_init(void);
extern void memstore_open(SMgrRelation reln);
extern void memstore_close(SMgrRelation reln, ForkNumber forknum);
extern void memstore_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool memstore_exists(SMgrRelation reln, ForkNumber forknum);
extern void memstore_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void memstore_extend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool memstore_prefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
extern void memstore_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
extern void memstore_write(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void memstore_writeback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber memstore_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void memstore_truncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
extern void memstore_immedsync(SMgrRelation reln, ForkNumber forknum);

#endif
