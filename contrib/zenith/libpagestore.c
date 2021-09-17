/*-------------------------------------------------------------------------
 *
 * libpqpagestore.c
 *	  Handles network communications with the remote pagestore.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	 contrib/zenith/libpqpagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pagestore_client.h"
#include "fmgr.h"
#include "access/xlog.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"

#include "replication/walproposer.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

#define PqPageStoreTrace DEBUG5

#define ZENITH_TAG "[ZENITH_SMGR] "
#define zenith_log(tag, fmt, ...) ereport(tag, \
		(errmsg(ZENITH_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

bool		connected = false;
PGconn	   *pageserver_conn;

static ZenithResponse * zenith_call(ZenithRequest *request);
page_server_api api = {
	.request = zenith_call
};

static void
zenith_connect()
{
	char			 *query;
	int				  ret;
	char			 *auth_token;
	char			 *err = NULL;
	PQconninfoOption *conn_options;
	PQconninfoOption *conn_option;
	int 			 noptions = 0;

    // this is heavily inspired by psql/command.c::do_connect
	conn_options = PQconninfoParse(
		page_server_connstring,
	 	&err
	);

	if (conn_options == NULL) {
		/* The error string is malloc'd, so we must free it explicitly */
		char	   *errcopy = err ? pstrdup(err) : "out of memory";
		PQfreemem(err);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("invalid connection string syntax: %s", errcopy)));
	}

	// Trying to populate pageserver connection string with auth token from environment.
	// We are looking for password in with placeholder value like $ENV_VAR_NAME, so if password field is present 
	// and starts with $ we try to fetch environment variable value and fail loudly if it is not set
	for (conn_option = conn_options; conn_option->keyword != NULL; conn_option++)
	{
		noptions++;
		if (strcmp(conn_option->keyword, "password") == 0)
		{
			if (conn_option->val != NULL && conn_option->val[0] != '\0')
			{
				// ensure that this is a template
				if (strncmp(conn_option->val, "$", 1) != 0) {
					ereport(
						ERROR,
						(
							errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("expected placeholder value in pageserver password starting from $ but found: %s", &conn_option->val[1])
						)
					);
				}
		
				zenith_log(LOG, "found auth token placeholder in pageserver conn string %s", &conn_option->val[1]);
				auth_token = getenv(&conn_option->val[1]);
				if (!auth_token) {
					ereport(
						ERROR,
						(
							errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("cannot get auth token, environment variable %s is not set", &conn_option->val[1])
						)
					);
				} else {
					zenith_log(LOG, "using auth token from environment passed via env");

				// inspired by PQconninfoFree and conninfo_storeval
				// so just free the old one and replace with freshly malloc'ed one
				free(conn_option->val);
				conn_option->val = strdup(auth_token);
				}
			}
		}
	}

	// copy values from PQconninfoOption to key/value arrays because PQconnectdbParams accepts options this way
	const char **keywords = malloc((noptions + 1) * sizeof(*keywords));
	const char **values = malloc((noptions + 1) * sizeof(*values));
	int			 i = 0;
	
	for (i = 0; i < noptions; i++)
	{
		keywords[i] = conn_options[i].keyword;
		values[i] = conn_options[i].val;
	}
	// add array terminator
	keywords[i] = NULL;
	values[i] = NULL;

	pageserver_conn = PQconnectdbParams(keywords, values, false);
	free(keywords);
	free(values);

	PQconninfoFree(conn_options);

	if (PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		PQfinish(pageserver_conn);
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("[ZENITH_SMGR] could not establish connection"),
				 errdetail_internal("%s", msg)));
	}

	/* Ask the Page Server to connect to us, and stream WAL from us. */
	if (callmemaybe_connstring && callmemaybe_connstring[0] 
		&& zenith_tenant
		&& zenith_timeline)
	{
		PGresult   *res;

		query = psprintf("callmemaybe %s %s %s", zenith_tenant, zenith_timeline, callmemaybe_connstring);
		res = PQexec(pageserver_conn, query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			zenith_log(ERROR,
					   "[ZENITH_SMGR] callmemaybe command failed");
		}
		PQclear(res);
	}

	query = psprintf("pagestream %s %s", zenith_tenant, zenith_timeline);
	ret = PQsendQuery(pageserver_conn, query);
	if (ret != 1)
		zenith_log(ERROR,
				   "[ZENITH_SMGR] failed to start dispatcher_loop on pageserver");

	while (PQisBusy(pageserver_conn))
	{
		int			wc;

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_SOCKET_READABLE |
							   WL_EXIT_ON_PM_DEATH,
							   PQsocket(pageserver_conn),
							   -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
				zenith_log(ERROR, "[ZENITH_SMGR] failed to get handshake from pageserver: %s",
						   PQerrorMessage(pageserver_conn));
		}
	}

	zenith_log(LOG, "libpqpagestore: connected to '%s'", page_server_connstring);

	connected = true;
}


static ZenithResponse *
zenith_call(ZenithRequest *request)
{
	StringInfoData req_buff;
	StringInfoData resp_buff;
	ZenithMessage *resp;

	/* If the connection was lost for some reason, reconnect */
	if (connected && PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		connected = false;
	}

	if (!connected)
		zenith_connect();

	req_buff = zm_pack_request(request);

	/* send request */
	if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) <= 0 || PQflush(pageserver_conn))
	{
		zenith_log(ERROR, "failed to send page request: %s",
				   PQerrorMessage(pageserver_conn));
	}
	pfree(req_buff.data);

	if (message_level_is_interesting(PqPageStoreTrace))
	{
		char	   *msg = zm_to_string((ZenithMessage *) request);

		zenith_log(PqPageStoreTrace, "Sent request: %s", msg);
		pfree(msg);
	}

	/* read response */
	resp_buff.len = PQgetCopyData(pageserver_conn, &resp_buff.data, 0);
	resp_buff.cursor = 0;

	if (resp_buff.len == -1)
		zenith_log(ERROR, "end of COPY");
	else if (resp_buff.len == -2)
		zenith_log(ERROR, "could not read COPY data: %s", PQerrorMessage(pageserver_conn));

	resp = zm_unpack_response(&resp_buff);
	PQfreemem(resp_buff.data);

	Assert(messageTag(resp) == T_ZenithStatusResponse
		   || messageTag(resp) == T_ZenithNblocksResponse
		   || messageTag(resp) == T_ZenithReadResponse);

	if (message_level_is_interesting(PqPageStoreTrace))
	{
		char	   *msg = zm_to_string((ZenithMessage *) resp);

		zenith_log(PqPageStoreTrace, "Got response: %s", msg);
		pfree(msg);
	}

	/*
	 * XXX: zm_to_string leak strings. Check with what memory contex all this
	 * methods are called.
	 */

	return (ZenithResponse *) resp;
}


static bool
check_zenith_id(char **newval, void **extra, GucSource source)
{
	uint8		zid[16];

	return **newval == '\0' || HexDecodeString(zid, *newval, 16);
}

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("zenith.page_server_connstring",
							   "connection string to the page server",
							   NULL,
							   &page_server_connstring,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	DefineCustomStringVariable("zenith.callmemaybe_connstring",
							   "Connection string that Page Server or WAL safekeeper should use to connect to us",
							   NULL,
							   &callmemaybe_connstring,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	DefineCustomStringVariable("zenith.zenith_timeline",
							   "Zenith timelineid the server is running on",
							   NULL,
							   &zenith_timeline,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_zenith_id, NULL, NULL);

	DefineCustomStringVariable("zenith.zenith_tenant",
							   "Zenith tenantid the server is running on",
							   NULL,
							   &zenith_tenant,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_zenith_id, NULL, NULL);

	DefineCustomBoolVariable("zenith.wal_redo",
							 "start in wal-redo mode",
							 NULL,
							 &wal_redo,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	relsize_hash_init();

	if (page_server != NULL)
		zenith_log(ERROR, "libpqpagestore already loaded");

	zenith_log(PqPageStoreTrace, "libpqpagestore already loaded");
	page_server = &api;

	/* Is there more correct way to pass CustomGUC to postgres code? */
	zenith_timeline_walproposer = zenith_timeline;
	zenith_tenant_walproposer = zenith_tenant;

	if (wal_redo)
	{
		zenith_log(PqPageStoreTrace, "set inmem_smgr hook");
		smgr_hook = smgr_inmem;
		smgr_init_hook = smgr_init_inmem;
	}
	else if (page_server_connstring && page_server_connstring[0])
	{
		zenith_log(PqPageStoreTrace, "set zenith_smgr hook");
		smgr_hook = smgr_zenith;
		smgr_init_hook = smgr_init_zenith;
	}
}