#include "sapnwrfc.h"
#include <vector>
#include <stdio.h>

#ifdef SAPonHP_UX
namespace std {}
#endif


class RfcExecException : public std::exception
{
public:
	RfcExecException(void) : std::exception(), rc(0) {}
	RfcExecException(const SAP_UC* message) : std::exception(), message(message), rc(0) {}
	RfcExecException(const SAP_UC* message, int rc) : std::exception(), message(message), rc(rc) {}
	virtual ~RfcExecException(void) throw() {};

	const SAP_UC* message;
	int rc;
};


class RFC_REMOTE_EXEC_desc
{
public:
	static RFC_FUNCTION_DESC_HANDLE getRFC_REMOTE_EXEC_desc(void);
	static RFC_FUNCTION_DESC_HANDLE getRFC_REMOTE_PIPE_desc(void);
	static RFC_TYPE_DESC_HANDLE getPIPEDATA_desc(void);

	static void deleteAll(void);
protected:
	static void add_RFC_REMOTE_EXEC_parameters(RFC_FUNCTION_DESC_HANDLE handle);

	static RFC_FUNCTION_DESC_HANDLE RFC_REMOTE_EXEC;
	static RFC_FUNCTION_DESC_HANDLE RFC_REMOTE_PIPE;
	static RFC_TYPE_DESC_HANDLE PIPEDATA;
};

class Trace
{
public:
	static Trace& get(void);

	~Trace(void);
	Trace(Trace const&);// = delete;
	Trace& operator=(Trace const&);// = delete;

	bool isOpen(void) const;
	void open(void);
	void write(const SAP_UC* key, const SAP_UC* value, bool indent = false, bool newline = true);
	void close(void);
private:
	Trace(void);
	FILE* traceFile;
};


class SecurityEntry
{
public:
	SecurityEntry(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path);
	SecurityEntry(SecurityEntry const& other);
	SecurityEntry& operator=(SecurityEntry const& other);
	~SecurityEntry(void);

	SAP_UC* user;
	SAP_UC* sysid;
	SAP_UC* client;
	SAP_UC* path;

private:
	void allocAndCopy(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path);
};


class Authorization
{
public:
	virtual ~Authorization(void) {}

	virtual bool isExecutionAllowed(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const = 0;
	virtual void traceSecurityInfo(void) const = 0;
	void traceExecutionParameters(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const;
};

class DefaultAuthorization : public Authorization
{
public:
	DefaultAuthorization(const SAP_UC* system);
	virtual ~DefaultAuthorization(void);

	virtual bool isExecutionAllowed(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const;
	virtual void traceSecurityInfo(void) const;
private:
	const SAP_UC* system;
};


class SecFileAuthorization : public Authorization
{
public:
	SecFileAuthorization(void);
	~SecFileAuthorization(void);

	virtual bool isExecutionAllowed(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const;
	virtual void traceSecurityInfo(void) const;

	void parseSecFile(const SAP_UC* filePath);
private:
	std::vector<SecurityEntry> allowed;
};


class RfcExecServer
{
public:
	static void usage(const SAP_UC* param);
	static bool isStartedServer(int argc, SAP_UC** argv);

	RfcExecServer(int argc, SAP_UC** argv);
	~RfcExecServer(void);

	void run(void);
	RFC_RC handleRequest(RFC_CONNECTION_HANDLE rfcHandle, RFC_FUNCTION_HANDLE funcHandle, RFC_ERROR_INFO *errorInfo);

	bool running;
	bool registered;
private:
    std::vector<RFC_CONNECTION_PARAMETER> connectionParams;
	RFC_CONNECTION_HANDLE connection;
	SAP_UC system[9];

	Authorization* authorization;
};
