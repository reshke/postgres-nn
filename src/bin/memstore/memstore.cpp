extern "C" {
#include <postgres.h>
#include <storage/relfilenode.h>
#include <storage/smgr.h>
#include <storage/memstore.h>
}

#include <sys/stat.h>
#include "common/ip.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <errno.h>

#include <map>
#include <mutex>
#include <thread>
#include <iostream>

struct BlockData
{
	char data[BLCKSZ];

	BlockData() {
		memset(data, 0, sizeof data);
	}
};

std::mutex mutex;
std::map<FileRef,BlockNumber> files;
std::map<BlockRef,BlockData> blocks;

inline int CompareFileRef(FileRef const& r1, FileRef const& r2)
{
	if (r1.rnode.node.relNode != r2.rnode.node.relNode)
		return r1.rnode.node.relNode - r2.rnode.node.relNode;
	if (r1.rnode.node.dbNode != r2.rnode.node.dbNode)
		return r1.rnode.node.dbNode - r2.rnode.node.dbNode;
	if (r1.rnode.backend != r2.rnode.backend)
		return r1.rnode.backend - r2.rnode.backend;
	if (r1.fork != r2.fork)
		return r1.fork - r2.fork;
	return r1.rnode.node.spcNode - r2.rnode.node.spcNode;
}



inline bool operator < (FileRef const& r1, FileRef const& r2)
{
	return CompareFileRef(r1, r2) < 0;
}

inline bool operator < (BlockRef const& r1, BlockRef const& r2)
{
	int diff = CompareFileRef(r1.file, r2.file);
	return diff != 0 ? diff < 0 : r1.blkno < r2.blkno;
}

static pgsocket CreateSocket(char const* host, char const* port, int n_peers);


void serve(int sock)
{
	MemstoreRequest req;
	BlockData* block;
	while (true)
	{
		size_t rc = read(sock, &req, sizeof req);
		if (rc != sizeof req) {
			std::cerr << "Failed to read request: " << strerror(errno) << "\n";
			close(sock);
			return;
		}
		switch (req.cmd)
		{
		  case MS_UNLINK:
		  {
			  std::lock_guard<std::mutex> guard(mutex);
			  files.erase(req.ref.file);
			  BlockRef first = req.ref;
			  first.blkno = 0;
			  BlockRef last = req.ref;
			  last.file.rnode.node.spcNode += 1;
			  last.blkno = 0;
			  auto from = blocks.find(req.ref);
			  if (from != blocks.end()) {
				  blocks.erase(from, blocks.lower_bound(last));
			  }
			  break;
		  }
		  case MS_CREATE:
		  {
			  std::lock_guard<std::mutex> guard(mutex);
			  files[req.ref.file] = 0;
			  break;
		  }
		  case MS_READ:
		  {
			  {
				  std::lock_guard<std::mutex> guard(mutex);
				  block = &blocks[req.ref];
			  }
			  rc = write(sock, block->data, BLCKSZ);
			  if (rc != BLCKSZ) {
				  std::cerr << "Failed to write data: " << strerror(errno) << "\n";
				  close(sock);
				  return;
			  }
			  break;
		  }
		  case MS_WRITE:
		  {
			  {
				  std::lock_guard<std::mutex> guard(mutex);
				  block = &blocks[req.ref];
				  BlockNumber& size = files[req.ref.file];
				  if (size <= req.ref.blkno) {
					  size = req.ref.blkno+1;
				  }
			  }
			  rc = read(sock, block->data, BLCKSZ);
			  if (rc != BLCKSZ) {
				  std::cerr << "Failed to read data: " << strerror(errno) << "\n";
				  close(sock);
				  return;
			  }
			  else
			  {
				  bool ok = true;
				  rc = write(sock, &ok, sizeof ok);
				  if (rc != sizeof ok) {
					  std::cerr << "Failed to write status: " << strerror(errno) << "\n";
					  close(sock);
					  return;
				  }
			  }
			  break;
		  }
		  case MS_SIZE:
		  {
			  BlockNumber size;
			  {
				  std::lock_guard<std::mutex> guard(mutex);
				  size = files[req.ref.file];
			  }
			  rc = write(sock, &size, sizeof(size));
			  if (rc != sizeof(size)) {
				  std::cerr << "Failed to send response: " << strerror(errno) << "\n";
				  close(sock);
				  return;
			  }
			  break;
		  }
		  case MS_EXISTS:
		  {
			  bool exists;
			  {
				  std::lock_guard<std::mutex> guard(mutex);
				  exists = files.find(req.ref.file) != files.end();
			  }
			  rc = write(sock, &exists, sizeof(exists));
			  if (rc != sizeof(exists)) {
				  std::cerr << "Failed to send response: " << strerror(errno) << "\n";
				  close(sock);
				  return;
			  }
			  break;
		  }
		  case MS_TRUNC:
		  {
			  std::lock_guard<std::mutex> guard(mutex);
			  files[req.ref.file] = req.ref.blkno;
			  BlockRef last = req.ref;
			  last.file.rnode.node.spcNode += 1;
			  last.blkno = 0;
			  auto from = blocks.find(req.ref);
			  if (from != blocks.end()) {
				  blocks.erase(from, blocks.lower_bound(last));
			  }
			  break;
		  }
		  default:
			Assert(false);
		}
	}
}

int main(int argc, char* argv[])
{
	int gateway = CreateSocket(MEMSTORE_HOST, MEMSTORE_PORT, 100);

	while (true)
	{
		int sock = accept(gateway, NULL, NULL);
		if (sock >= 0)
		{
			new std::thread(serve,sock);
		}
	}
	return 0;
}

static bool
SetSocketOptions(pgsocket sock)
{
	int on = 1;
	if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
				   (char *) &on, sizeof(on)) < 0)
	{
		std::cerr << "setsockopt(TCP_NODELAY) failed: " << 	strerror(errno) << "\n";
		closesocket(sock);
		return false;
	}
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
				   (char *) &on, sizeof(on)) == -1)
	{
		std::cerr << "setsockopt(SO_REUSEADDR) failed: " << 	strerror(errno) << "\n";
		closesocket(sock);
		return false;
	}
	return true;
}

pgsocket
CreateSocket(char const* host, char const* port, int n_peers)
{
	struct addrinfo *addrs = NULL,
		*addr,
		hints;
	int	ret;
	pgsocket sock = PGINVALID_SOCKET;

	/*
	 * Create the UDP socket for sending and receiving statistic messages
	 */
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
		std::cerr << "Could not resolve " << host << ": " << strerror(errno) << "\n";
		return -1;
	}
	for (addr = addrs; addr; addr = addr->ai_next)
	{
		sock = socket(addr->ai_family, SOCK_STREAM, 0);
		if (sock == PGINVALID_SOCKET)
		{
			std::cerr << "Could not create socket: " << strerror(errno) << "\n";
			continue;
		}
		/*
		 * Bind it to a kernel assigned port on localhost and get the assigned
		 * port via getsockname().
		 */
		if (bind(sock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			std::cerr << "Could not bind socket: " << strerror(errno) << "\n";
			closesocket(sock);
			continue;
		}
		ret = listen(sock, n_peers);
		if (ret < 0)
		{
			std::cerr << "Could not listen socket: " << strerror(errno) << "\n";
			closesocket(sock);
			continue;
		}
		if (SetSocketOptions(sock))
			break;
	}
	return sock;
}
