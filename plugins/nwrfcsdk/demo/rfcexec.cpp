#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <algorithm>


#ifdef SAPonNT
#include <direct.h>
#include <process.h>
#include <windows.h>
#include <tchar.h>
#include <io.h>
#else
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#endif

#ifdef SAPwithPASE400
#include "as4exec.h"
#define SAPUCX_H
#endif
#include "rfcexec.h"

#ifdef SAPonOS400
#include <spawn.h>

#ifdef SAPwithCHAR_EBCDIC
#include <qp0z1170.h>     // weil’s der CPP Compiler im Gegensatz zum C Compiler für GetEnvironU braucht
#endif

externC int o4_convert_environ_a(void);
DECLAREenvironU;
#endif
#if defined(SAPonOS400)
/* o4fprintfU, o4fgetsU
 * calling o4xxxU instead of xxxU produces much smaller code,
 * because it directly expands to xxxU16, while xxxU expands to
 * as4_xxx  which links against the whole pipe check and handling for ILE.
 * This creates an executable containing almost the whole platform 
 * specific kernel and needs the ILE O4PRTLIB in a special library.
 * Because we have no pipe usage of fxxxU here I use o4xxxU.
 * ATTENTION:
 * In any case the above mentioned functions are efectively used for 
 * pipes, the redefinition below corrupts this functionality
 * because than the pipe handling is not called for our platform.
 */
#undef fprintfU
#define fprintfU o4fprintfU
#undef fgetsU
#define fgetsU o4fgetsU
#endif

#ifdef SAPonNT
#    ifndef pclose
#        define pclose _pclose
#    endif
#endif

#ifdef SAPonNT
#ifndef R_OK
#define R_OK 4
#endif // !R_OK
#ifndef F_OK
#define F_OK 0
#endif // !F_OK
#endif

#define ERROR_MESSAGE_SIZE 511


/**
* \brief Helper function to remove leading whitespaces
* \ingroup rfcexec
*
*/
SAP_UC* ltrim(SAP_UC* s) {
	while (isspaceU(*s))
		s++;
	return s;
}

/**
* \brief Helper function to remove trailing whitespaces
* \ingroup rfcexec
*
*/
SAP_UC* rtrim(SAP_UC* s) {
	SAP_UC* back = s + strlenU(s);
	while (isspaceU(*--back))
		;
	*(back + 1) = cU('\0');
	return s;
}

/**
* \brief  Creates a hard-coded metadata description of the FM RFC_REMOTE_EXEC as used by
* the ALE interface.
* \ingroup rfcexec
*
* Note that the table PIPEDATA has been implemented by the original rfcexec program, but is
* not used in the ALE standard functionality. Other EDI subsystems may use it though, therefore
* it remains part of the interface.
*/

/* static */ RFC_FUNCTION_DESC_HANDLE RFC_REMOTE_EXEC_desc::RFC_REMOTE_EXEC = NULL;
/* static */ RFC_FUNCTION_DESC_HANDLE RFC_REMOTE_EXEC_desc::RFC_REMOTE_PIPE = NULL;
/* static */ RFC_TYPE_DESC_HANDLE RFC_REMOTE_EXEC_desc::PIPEDATA = NULL;


/* static */ RFC_FUNCTION_DESC_HANDLE RFC_REMOTE_EXEC_desc::getRFC_REMOTE_EXEC_desc(void) {
	if (RFC_REMOTE_EXEC == NULL) {
		/* in multithread environment we need to add mutex protection here, and try-catch */
		if (RFC_REMOTE_EXEC == NULL) {
			RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
			RFC_REMOTE_EXEC = RfcCreateFunctionDesc(cU("RFC_REMOTE_EXEC"), &errorInfo);
			if (RFC_REMOTE_EXEC == NULL)
				throw RfcExecException(errorInfo.message, errorInfo.code);
			add_RFC_REMOTE_EXEC_parameters(RFC_REMOTE_EXEC);
		}
	}

	return RFC_REMOTE_EXEC;
}
/* static */ RFC_FUNCTION_DESC_HANDLE RFC_REMOTE_EXEC_desc::getRFC_REMOTE_PIPE_desc(void) {
	if (RFC_REMOTE_PIPE == NULL) {
		/* in multithread environment we need to add mutex protection here, and try-catch */
		if (RFC_REMOTE_PIPE == NULL) {
			RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
			RFC_REMOTE_PIPE = RfcCreateFunctionDesc(cU("RFC_REMOTE_PIPE"), &errorInfo);
			if (RFC_REMOTE_PIPE == NULL)
				throw RfcExecException(errorInfo.message, errorInfo.code);
			add_RFC_REMOTE_EXEC_parameters(RFC_REMOTE_PIPE);
		}
	}
	return RFC_REMOTE_PIPE;
}
/* static */ RFC_TYPE_DESC_HANDLE RFC_REMOTE_EXEC_desc::getPIPEDATA_desc(void) {
	if (PIPEDATA == NULL) {
		/* in multithread environment we need to add mutex protection here, and try-catch */
		if (PIPEDATA == NULL) {
			RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
			PIPEDATA = RfcCreateTypeDesc(cU("PIPEDATA"), &errorInfo);
			if (PIPEDATA == NULL)
				throw RfcExecException(errorInfo.message, errorInfo.code);

			RFC_FIELD_DESC fieldDescr = RFC_FIELD_DESC();

			strncpy_sU(fieldDescr.name, sizeofU(fieldDescr.name), cU("PIPEDATA"), 9);
			fieldDescr.type = RFCTYPE_CHAR;
			fieldDescr.nucLength = 80;
			fieldDescr.nucOffset = 0;
			fieldDescr.ucLength = 160;
			fieldDescr.ucOffset = 0;
			fieldDescr.decimals = 0;
			fieldDescr.typeDescHandle = NULL;
			fieldDescr.extendedDescription = NULL;
			RFC_RC rc = RfcAddTypeField(PIPEDATA, &fieldDescr, &errorInfo);
			if (rc != RFC_OK)
				throw RfcExecException(errorInfo.message, errorInfo.code);

			rc = RfcSetTypeLength(PIPEDATA, 80, 160, &errorInfo);
			if (rc != RFC_OK)
				throw RfcExecException(errorInfo.message, errorInfo.code);
		}
	}
	return PIPEDATA;
}
/* static */ void RFC_REMOTE_EXEC_desc::add_RFC_REMOTE_EXEC_parameters(RFC_FUNCTION_DESC_HANDLE handle) {
	RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();

	{
		RFC_PARAMETER_DESC paramDescr = RFC_PARAMETER_DESC();
		strncpy_sU(paramDescr.name, sizeofU(paramDescr.name), cU("COMMAND"), 8);
		paramDescr.type = RFCTYPE_CHAR;
		paramDescr.direction = RFC_IMPORT;
		paramDescr.nucLength = 256;
		paramDescr.ucLength = 512;
		paramDescr.decimals = 0;
		paramDescr.typeDescHandle = NULL;
		const RFC_RC rc = RfcAddParameter(handle, &paramDescr, &errorInfo);
		if (rc != RFC_OK)
			throw RfcExecException(errorInfo.message, errorInfo.code);
	}

	{
		RFC_PARAMETER_DESC paramDescr = RFC_PARAMETER_DESC();
		strncpy_sU(paramDescr.name, sizeofU(paramDescr.name), cU("READ"), 5);
		paramDescr.type = RFCTYPE_CHAR;
		paramDescr.direction = RFC_IMPORT;
		paramDescr.nucLength = 1;
		paramDescr.ucLength = 2;
		paramDescr.decimals = 0;
		paramDescr.typeDescHandle = NULL;
		const RFC_RC rc = RfcAddParameter(handle, &paramDescr, &errorInfo);
		if (rc != RFC_OK)
			throw RfcExecException(errorInfo.message, errorInfo.code);
	}

	{
		RFC_PARAMETER_DESC paramDescr = RFC_PARAMETER_DESC();
		strncpy_sU(paramDescr.name, sizeofU(paramDescr.name), cU("PIPEDATA"), 9);
		paramDescr.type = RFCTYPE_TABLE;
		paramDescr.direction = RFC_TABLES;
		paramDescr.nucLength = 8;
		paramDescr.ucLength = 8;
		paramDescr.decimals = 0;
		paramDescr.typeDescHandle = getPIPEDATA_desc();
		const RFC_RC rc = RfcAddParameter(handle, &paramDescr, &errorInfo);
		if (rc != RFC_OK)
			throw RfcExecException(errorInfo.message, errorInfo.code);
	}
}
/* static */ void RFC_REMOTE_EXEC_desc::deleteAll(void) {

	RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
	if (RFC_REMOTE_EXEC) {
		RfcDestroyFunctionDesc(RFC_REMOTE_EXEC, &errorInfo);
		RFC_REMOTE_EXEC = NULL;
	}
	if (RFC_REMOTE_PIPE) {
		RfcDestroyFunctionDesc(RFC_REMOTE_PIPE, &errorInfo);
		RFC_REMOTE_PIPE = NULL;
	}
	if (PIPEDATA) {
		RfcDestroyTypeDesc(PIPEDATA, &errorInfo);
		PIPEDATA = NULL;
	}
}


/**
 * \brief  Getter for the trace singleton.
 * \ingroup rfcexec
 *
 */
/* static */ Trace& Trace::get(void) {
	static Trace instance;
	return instance;
}
/**
 * \brief  Constructor of trace singleton.
 * \ingroup rfcexec
 *
 */
Trace::Trace(void) : traceFile(NULL) {
}
/**
 * \brief  Destructor of trace singleton.
 * \ingroup rfcexec
 *
 * Will be called when the program ends.
 */
Trace::~Trace(void) {
	close();
}

/**
 * \brief  Checks if the traceFile was opened before.
 * \ingroup rfcexec
 *
 */
bool Trace::isOpen(void) const {
	return traceFile != NULL;
}
/**
 * \brief  Opens the trace file.
 * \ingroup rfcexec
 *
 * We don't throw an error, if opening the trace file fails, because tracing should
 * not disrupt the productive functionality of the program.
 */
void Trace::open(void) {
	if (traceFile == NULL) {
		const time_t currTime = time(NULL);
		SAP_UC tracefile_name[128] = iU("");
		sprintf_sU(tracefile_name, sizeofU(tracefile_name), /*CCQ_FORMAT_STRING_OK*/cU("rfcexec%.5d_%05") SAP_Fllu cU(".trc"), getpid(), static_cast<SAP_ULLONG>(currTime));
		traceFile = fopenU(tracefile_name, cU("wt"));

		if (traceFile) {
			SAP_UC cwd[512] = iU(""); /*CCQ_CLIB_LOCTIME_OK*/
			write(cU("***** Rfcexec trace file opened at"), ctimeU(&currTime));
			write(cU("NW RFC SDK Version"), RfcGetVersion(NULL, NULL, NULL));
			getcwdU(cwd, sizeofU(cwd));
			write(cU("***** Current working directory: "), cwd);
		}
	}
}
/**
 * \brief  Writes an entry to the trace file.
 * \ingroup rfcexec
 *
 * We don't throw an error, if writing the trace entry fails, because tracing should
 * not disrupt the productive functionality of the program.
 *
 * \in key The "variable name" of the variable to trace, or a "heading".
 * \in value The value of the variable to trace, or a "message".
 * \in indent Start the line with a tabs. Optional. Default is off.
 */
void Trace::write(const SAP_UC* key, const SAP_UC* value, bool indent, bool newline) {
	if (traceFile) {
		if (indent)
			fprintfU(traceFile, cU("\t"));

		fprintfU(traceFile, cU("%s"), key);
		if (value)
			fprintfU(traceFile, cU(":\t%s"), value);

		if (newline)
			fprintfU(traceFile, cU("\n"));
		fflush(traceFile);
	}
}
/**
 * \brief  Closes the trace file.
 * \ingroup rfcexec
 */
void Trace::close(void) {
	if (traceFile) {
		const time_t currTime = time(NULL);
		/*CCQ_CLIB_LOCTIME_OK*/
		fprintfU(traceFile, cU("***** Rfcexec trace file closed at %s\n"), ctimeU(&currTime));
		fclose(traceFile);
		traceFile = NULL;
	}
}


SecurityEntry::SecurityEntry(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path) :
user(NULL), sysid(NULL), client(NULL), path(NULL) {
	allocAndCopy(user, sysid, client, path);
}
SecurityEntry::SecurityEntry(SecurityEntry const& other) {
	allocAndCopy(other.user, other.sysid, other.client, other.path);
}
SecurityEntry& SecurityEntry::operator=(SecurityEntry const& other) {
	allocAndCopy(other.user, other.sysid, other.client, other.path);

	return *this;
}
SecurityEntry::~SecurityEntry(void) {
	if (user) {
		delete[] user;
		user = NULL;
	}
	if (sysid) {
		delete[] sysid;
		sysid = NULL;
	}
	if (client) {
		delete[] client;
		client = NULL;
	}
	if (path) {
		delete[] path;
		path = NULL;
	}
}
void SecurityEntry::allocAndCopy(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path) {
	size_t len = strlenU(user);
	this->user = new SAP_UC[len + 1]();
	strncpyU(this->user, user, len);

	len = strlenU(sysid);
	this->sysid = new SAP_UC[len + 1]();
	strncpyU(this->sysid, sysid, len);

	len = strlenU(client);
	this->client = new SAP_UC[len + 1]();
	strncpyU(this->client, client, len);

	len = strlenU(path);
	this->path = new SAP_UC[len + 1]();
	strncpyU(this->path, path, len);
}


void Authorization::traceExecutionParameters(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const {
	if (user)
		Trace::get().write(cU("User"), user, true);
	if (sysid)
		Trace::get().write(cU("SysID"), sysid, true);
	if (client)
		Trace::get().write(cU("Client"), client, true);
	if (path)
		Trace::get().write(cU("Program"), path, true);
	if (caller)
		Trace::get().write(cU("Calling Report"), caller, true);
}

DefaultAuthorization::DefaultAuthorization(const SAP_UC* sys) : Authorization() {
	system = sys;
}
DefaultAuthorization::~DefaultAuthorization(void) {
}

/**
 * \brief  Checks whether the current SAP user is allowed to execute the given OS command.
 * \ingroup rfcexec
 *
 * If the program has been started with the extra parameter -f \<filename>, the given user,
 * SAP system ID, client and OS command are matched against the cached entries of the file.
 * Access is allowed, only if an exact match can be found.
 *
 * Otherwise the program verifies only, that the call came from the correct SAP system and
 * that the calling program is the ALE layer (SAPLEDI7).
 *
 * \in user The current backend user calling the server program
 * \in sysid System ID of the calling backend
 * \in client The Client ("Mandant", not the opposite of "Server"...!) from which the call came
 * \in path The program to be started, given by relative or absolute path. Came from the
 * function module import parameter COMMAND
 * \in caller Name of the ABAP Report/Function Group, which issued the CALL FUNCTION statement.
 * Used only in "default mode".
 * \return true means: this call is allowed, false means: it's denied.
 */
bool DefaultAuthorization::isExecutionAllowed(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const {
	traceExecutionParameters(user, sysid, client, path, caller);

	bool isAuthorized = false;
	if (caller  && strncmpU(caller, cU("SAPLEDI7"), strlenU(caller)) == 0 && (*system == cU('\0') || (sysid && strncmpU(sysid, system, strlenU(sysid)) == 0)))
		isAuthorized = true;

	return isAuthorized;
}
/**
 * \brief  Prints the used security settings to the trace file.
 * \ingroup rfcexec
 */
void DefaultAuthorization::traceSecurityInfo(void) const {
	Trace::get().write(cU("Using default mode. Allowing connections only from Report SAPLEDI7 and System"), system);
	Trace::get().write(cU("\t----------\n"), NULL);
}



SecFileAuthorization::SecFileAuthorization(void) : Authorization() {
}
SecFileAuthorization::~SecFileAuthorization(void) {
}

/**
 * \brief  Checks whether the current SAP user is allowed to execute the given OS command.
 * \ingroup rfcexec
 *
 * If the program has been started with the extra parameter -f \<filename>, the given user,
 * SAP system ID, client and OS command are matched against the cached entries of the file.
 * Access is allowed, only if an exact match can be found.
 *
 * Otherwise the program verifies only, that the call came from the correct SAP system and
 * that the calling program is the ALE layer (SAPLEDI7).
 *
 * \in user The current backend user calling the server program
 * \in sysid System ID of the calling backend
 * \in client The Client ("Mandant", not the opposite of "Server"...!) from which the call came
 * \in path The program to be started, given by relative or absolute path. Came from the
 * function module import parameter COMMAND
 * \in caller Name of the ABAP Report/Function Group, which issued the CALL FUNCTION statement.
 * Used only in "default mode".
 * \return true means: this call is allowed, false means: it's denied.
 */
bool SecFileAuthorization::isExecutionAllowed(const SAP_UC* user, const SAP_UC* sysid, const SAP_UC* client, const SAP_UC* path, const SAP_UC* caller) const {
	traceExecutionParameters(user, sysid, client, path, caller);

	bool isAuthorized = false;
	for (std::vector<SecurityEntry>::const_iterator it = allowed.begin(); it != allowed.end(); ++it) {
		if (user && strncmpU(user, it->user, strlenU(user)) == 0
			&& sysid && strncmpU(sysid, it->sysid, strlenU(sysid)) == 0
			&& client && strncmpU(client, it->client, strlenU(client)) == 0
			&& path && (strncmpU(cU("*"), it->path, 1) == 0 || strncmpU(path, it->path, strlenU(path)))) {
			isAuthorized = true;
			break;
		}
	}
	return isAuthorized;
}
/**
 * \brief  Prints the used security settings to the trace file.
 * \ingroup rfcexec
 */
void SecFileAuthorization::traceSecurityInfo(void) const {
	Trace::get().write(cU("Using secure mode. Allowing connections only for the following USER:SYSID:CLIENT:PROGRAM combinations"), NULL);
	for (std::vector<SecurityEntry>::const_iterator it = allowed.begin(); it != allowed.end(); ++it) {
		Trace::get().write(cU("USER="), it->user, true, false);
		Trace::get().write(cU(", SYSID="), it->sysid, true, false);
		Trace::get().write(cU(", CLIENT="), it->client, true, false);
		Trace::get().write(cU(", PATH="), it->path, true);
	}
	Trace::get().write(cU("\t----------\n"), NULL);
}
/**
 * \brief  Parses the file containing detailed access restrictions for this program.
 * \ingroup rfcexec
 *
 * This program can be started with an additional file containing detailed information about
 * which SAP user from which R/3 system and which client may execute which OS command.
 * Each line of the file must be in the following format:
 *
 * USER=EDIUSER,SYSID=XYZ,CLIENT=000,PATH=/usr/bin/edi_sub_system.sh
 *
 * \in *filePath Path and filename specifying the file to read.
 */
void SecFileAuthorization::parseSecFile(const SAP_UC* filePath) {
	FILE* secFile = NULL;
	secFile = fopenU(filePath, cU("rt"));

	if (secFile == NULL)
		throw RfcExecException(cU("parseSecFile: Could not open sec file"), errno);

	Trace::get().write(cU("Reading command file"), filePath, true);

	unsigned line = 0;
	SAP_UC buf[10025] = iU("");
	while (fgetsU(buf, sizeofU(buf), secFile)) {
		line++;
		Trace::get().write(buf, NULL, false, false);
		size_t lineLength = strlenU(buf);

		// ignore comments starting with "//" or "#" and empty lines
		if (lineLength == 0 || strncmpU(buf, cU("\0"), 1) == 0 || strncmpU(buf, cU("\n"), 1) == 0 || strncmpU(buf, cU("//"), 2) == 0 || strncmpU(buf, cU("#"), 1) == 0)
			continue;
		else if (lineLength == 10024 && buf[10023] != cU('\n')) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Line no. %d is too long. Limit: 10024"), line);
			throw RfcExecException(buf, -1);
		}
		else if (lineLength < 30) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Line no. %d is invalid."), line);
			throw RfcExecException(buf, -2);
		}

		if (buf[lineLength - 1] == cU('\n'))
			buf[--lineLength] = cU('\0');

		const SAP_UC* user = NULL;
		const SAP_UC* sysid = NULL;
		const SAP_UC* client = NULL;
		const SAP_UC* path = NULL;
		SAP_UC* token = strtokU(buf, cU(","));
		while (token) {
			SAP_UC* trimmedToken = rtrim(ltrim(token));
			if (strncmpU(trimmedToken, cU("USER="), 5) == 0)
				user = trimmedToken + 5;
			else if (strncmpU(trimmedToken, cU("SYSID="), 6) == 0)
				sysid = trimmedToken + 6;
			else if (strncmpU(trimmedToken, cU("CLIENT="), 7) == 0)
				client = trimmedToken + 7;
			else if (strncmpU(trimmedToken, cU("PATH="), 5) == 0)
				path = trimmedToken + 5;
			else {
				sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Line no. %d contains invalid parameter."), line);
				throw RfcExecException(buf, -3);
			}
			token = strtokU(NULL, cU(","));
		}

		if (user == NULL) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Missing user in line no. %d"), line);
			throw RfcExecException(buf, -4);
		}
		if (sysid == NULL) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Missing sysid in line no. %d"), line);
			throw RfcExecException(buf, -5);
		}
		if (client == NULL) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Missing client in line no. %d"), line);
			throw RfcExecException(buf, -6);
		}
		if (path == NULL) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Missing path in line no. %d"), line);
			throw RfcExecException(buf, -7);
		}

		if (strlenU(sysid) > 8) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Invalid SYSID in line no. %d"), line);
			throw RfcExecException(buf, -8);
		}
		if (strlenU(client) != 3) {
			sprintf_sU(buf, sizeofU(buf), cU("parseSecFile: Invalid CLIENT in line no. %d"), line);
			throw RfcExecException(buf, -9);
		}

		allowed.push_back(SecurityEntry(user, sysid, client, path));
	}

	fclose(secFile);
	Trace::get().write(cU("End of file"), NULL, true);
}


Authorization* createAuthorization(bool isRegisteredServer, bool hasSecFile, const SAP_UC* secFilePath, const SAP_UC* system) {
	if (hasSecFile && accessU(secFilePath, F_OK | R_OK) != 0) {
		if (errno == ENOENT && !isRegisteredServer) {
			hasSecFile = false; // No file is ok, we use the default mode (ALE scenario).
			Trace::get().write(cU("No secFile available. Switching to Default security:"), strerrorU(errno));
		}
	}

	Authorization* authorization = NULL;
	if (hasSecFile) {
		SecFileAuthorization* secFileAuthorization = new SecFileAuthorization();
		secFileAuthorization->parseSecFile(secFilePath);
		authorization = secFileAuthorization;
	}
	else {
		authorization = new DefaultAuthorization(system);
	}
	return authorization;
}




RfcExecServer* theServer = NULL;

class StrCmp {
public:
	StrCmp(SAP_UC* str1) : str1(str1) {}
	bool operator()(const SAP_UC* str2) {
		return strcmpU(str1, str2) == 0;
	}
private:
	SAP_UC* str1;
};

/**
 * \brief  Callback function for the RFC SDK.
 * \ingroup rfcexec
 *
 * This is the "bridge" between the C RFC SDK and the C++ rfcexec program. If there will be more
 * than one parallel connection, then at this point instead of "theServer" we'll need either a kind
 * of hashmap mapping the RFC_CONNECTION_HANDLEs to the corresponding RfcExecServer objects, or
 * we'll need to use Thread Local Storage.
 *
 * Whenever an RFC request for the FM RFC_REMOTE_EXEC arrives from the backend, this function is
 * triggered.
 *
 * \in rfcHandle RFC connection over which the request came in.
 * \in funcHandle Data container for the inputs/outputs of the FM.
 * \out *errorInfo Returns detailed error information to the backend, in case anything goes wrong or access is denied.
 * \return RFC_OK, if everything went fine. RFC_EXTERNAL_FAILURE, if the parameters could not be read, access is
 * denied or the creation of the child process failed.
 */
extern "C" RFC_RC SAP_API RFC_REMOTE_EXEC(RFC_CONNECTION_HANDLE rfcHandle, RFC_FUNCTION_HANDLE funcHandle, RFC_ERROR_INFO *errorInfo) {
	return theServer->handleRequest(rfcHandle, funcHandle, errorInfo);
}

/**
 * \brief  Implementation of the "function module" RFC_REMOTE_EXEC.
 * \ingroup rfcexec
 *
 * Whenever an RFC request for the FM RFC_REMOTE_EXEC arrives from the backend, this function is
 * triggered and checks, whether the current user has authorization to execute the given OS command.
 * If access is granted, the command is executed in an asynchronous child process.
 * \note For performance reasons ALE doesn't want to wait for error/success output of the process,
 * therefore the PIPEDATA table is not used in this implementation.
 *
 * \in rfcHandle RFC connection over which the request came in.
 * \in funcHandle Data container for the inputs/outputs of the FM.
 * \out *errorInfo Returns detailed error information to the backend, in case anything goes wrong or access is denied.
 * \return RFC_OK, if everything went fine. RFC_EXTERNAL_FAILURE, if the parameters could not be read, access is
 * denied or the creation of the child process failed.
 */
RFC_RC RfcExecServer::handleRequest(RFC_CONNECTION_HANDLE rfcHandle, RFC_FUNCTION_HANDLE funcHandle, RFC_ERROR_INFO* errorInfo) {
	RFC_RC rc = RFC_OK;
	RFC_ATTRIBUTES attributes = RFC_ATTRIBUTES();
	SAP_UC command[256] = iU("");
	SAP_UC readPipe[2] = iU("");

	rc = RfcGetConnectionAttributes(rfcHandle, &attributes, errorInfo);
	if (rc != RFC_OK) {
		Trace::get().write(cU("Reading Attributes failed"), errorInfo->message, true);
		return RFC_EXTERNAL_FAILURE;
	}

	if (attributes.trace[0] != cU('0') && !Trace::get().isOpen()) {
		Trace::get().open();
		Trace::get().write(cU("Tracing turned on on backend's request"), NULL);
		authorization->traceSecurityInfo();
	}

	RFC_FUNCTION_DESC_HANDLE funcDesc = RfcDescribeFunction(funcHandle, errorInfo); // In addition to RFC_REMOTE_EXEC, we also support RFC_REMOTE_PIPE
	RfcGetFunctionName(funcDesc, command, errorInfo);
	Trace::get().write(cU("Received call for"), command);

	rc = RfcGetString(funcHandle, cU("COMMAND"), command, sizeofU(command), NULL, errorInfo);
	if (rc != RFC_OK) {
		Trace::get().write(cU("Reading COMMAND value failed"), errorInfo->message, true);
		return RFC_EXTERNAL_FAILURE;
	}

	rc = RfcGetString(funcHandle, cU("READ"), readPipe, sizeofU(readPipe), NULL, errorInfo);
	if (rc != RFC_OK) {
		Trace::get().write(cU("Reading READ value failed"), errorInfo->message, true);
		return RFC_EXTERNAL_FAILURE;
	}

	if (!authorization->isExecutionAllowed(attributes.user, attributes.sysId, attributes.client, command, attributes.progName)) {
		sprintf_sU(errorInfo->message, sizeofU(errorInfo->message), cU("Access denied"));
		errorInfo->code = RFC_EXTERNAL_FAILURE;
		Trace::get().write(cU("Access was denied"), NULL, true);
		return RFC_EXTERNAL_FAILURE;
	}

#ifdef SAPonOS400
	SAP_UC** args;
	int numArgs = 1;
	bool inArg = false;
	pid_t pid;
	inheritance_t inherit;
	size_t len, i;

	len = strlenU(command);
	for (i = 0; i < len; i++)
	{
		switch (command[i])
		{
		case cU('"'): inArg = !inArg; break;
		case cU(' '): if (inArg) continue;
			numArgs++;
			command[i] = cU('\0');
			break;
		}
	}
	args = new SAP_UC * [numArgs + 1];
	args[0] = command;
	numArgs = 1;
	for (i = 0; i < len; i++)
	{
		if (command[i] == cU('\0'))
		{
			args[numArgs++] = command + (i + 1);
			Trace::get().write(cU("args parameter"), command + (i + 1), true);
		}
	}
	args[numArgs] = NULL;

	SAP_UC numVal[16];
	sprintf_sU(numVal, sizeofU(numVal), cU("%d"), numArgs - 1);
	Trace::get().write(cU("Number of arguments"), numVal, true);

	if (getenvU(cU("PATH")) == NULL)
		putenvU(cU("PATH=%LIBL%:"));
	Trace::get().write(cU("Using PATH"), getenvU(cU("PATH")), true);

	inherit.flags = 0;  /* initialize inheritance structure */
	inherit.pgroup = SPAWN_NEWPGROUP;

	GETenvironU;
	if (environU == NULL)
	{
		SAP_UC* envvar_array[1] = { NULL };
		pid = spawnU(command, 0, NULL, &inherit, args, envvar_array);
	}
	else
	{
		pid = spawnU(command, 0, NULL, &inherit, args, environU);
	}

	delete[] args;
	if (pid == -1)
	{
		strncpy_sU(errorInfo->message, sizeofU(errorInfo->message), strerrorU(errno), ERROR_MESSAGE_SIZE);
		errorInfo->message[ERROR_MESSAGE_SIZE] = 0;
		errorInfo->code = RFC_EXTERNAL_FAILURE;
		Trace::get().write(cU("Starting process failed"), errorInfo->message, true);
		return RFC_EXTERNAL_FAILURE;
	}
	else
		Trace::get().write(cU("Process started successfully"), NULL, true);
#else
	FILE* pipe = popenU(command, cU("r"));

	if (pipe == NULL) {
		strncpy_sU(errorInfo->message, sizeofU(errorInfo->message), strerrorU(errno), ERROR_MESSAGE_SIZE);
		errorInfo->message[ERROR_MESSAGE_SIZE] = 0;
		errorInfo->code = RFC_EXTERNAL_FAILURE;
		Trace::get().write(cU("Starting process failed"), errorInfo->message, true);
		return RFC_EXTERNAL_FAILURE;
	}
	else
		Trace::get().write(cU("Process started successfully"), NULL, true);

	try {
		if (*readPipe == cU('X')) {
			RFC_TABLE_HANDLE pipedata;

			rc = RfcGetTable(funcHandle, cU("PIPEDATA"), &pipedata, errorInfo);
			if (rc != RFC_OK) {
				Trace::get().write(cU("Getting PIPEDATA table failed"), errorInfo->message, true);
				throw RfcExecException(errorInfo->message, errorInfo->code);
			}

			while (fgetsU(command, 80, pipe)) {
				RfcAppendNewRow(pipedata, errorInfo);
				if (errorInfo->code != RFC_OK) {
					Trace::get().write(cU("Appending row to PIPEDATA table failed"), errorInfo->message, true);
					throw RfcExecException(errorInfo->message, errorInfo->code);
				}

				// remove newline and tabs from the end
				size_t str_len = strlenU(command);
				if (command[str_len - 2] == cU('\r') && command[str_len - 1] == cU('\n'))
					str_len -= 2;
				else if (command[str_len - 1] == cU('\n') || command[str_len - 1] == cU('\t') || command[str_len - 1] == cU('\r'))
					--str_len;

				// replace tab by a space (may occur in the middle of the line, e.g. with the command "dir" on UNIX)
				SAP_UC* current_pos = strchrU(command, cU('\t'));
				while (current_pos) {
					*current_pos = cU(' ');
					current_pos = strchrU(current_pos, cU('\t'));
				}

				RfcSetChars(pipedata, cU("PIPEDATA"), command, str_len, errorInfo);
				if (errorInfo->code != RFC_OK) {
					Trace::get().write(cU("Setting PIPEDATA table failed"), errorInfo->message, true);
					throw RfcExecException(errorInfo->message, errorInfo->code);
				}
			}
		}
	}
	catch (const RfcExecException&) {
		rc = RFC_EXTERNAL_FAILURE;
	}

	while (fgetsU(command, 80, pipe));
	pclose(pipe);
#endif
	return rc;
}

/**
 * \brief  Stops the RFC server, when program is ended.
 * \ingroup rfcexec
 */
#ifdef SAPonNT
BOOL WINAPI shutdownHandler(DWORD /*dwCtrlType*/)
{
	theServer->running = false;
	Sleep(2000);
	return FALSE;
}
#else
extern "C" void shutdownHandler(int /*sig*/)
{
	theServer->running = false;
}
#endif


void enableTraceIfAny(int argc, SAP_UC** argv) {
	for (int i = 1; i < argc; i++) {
		const SAP_UC* arg = argv[i];
		if (strncmpU(cU("-t"), arg, 2) == 0) {
			Trace::get().open();
		}
		if (strncmpU(cU("CPIC_TRACE="), arg, 11) == 0 && arg[11] > cU('0')) {
			const SAP_UC* level = &arg[11];
			RfcSetCpicTraceLevel(atoiU(level), NULL);
		}
		if (strncmpU(cU("RFC_TRACE="), arg, 10) == 0 && arg[10] > cU('0')) {
			const SAP_UC* level = &arg[10];
			RfcSetTraceLevel(NULL, NULL, atoiU(level), NULL);
		}
	}
	if (Trace::get().isOpen()) {
		Trace::get().write(cU("Program started with:"), NULL);
		for (int i = 1; i < argc; ++i)
			Trace::get().write(argv[i], NULL);
	}
}

/**
 * \brief  Starts the program
 * \ingroup rfcexec
 *
 * Either started from the command line (registered mode) or by the gateway process (started mode).
 * In both cases the program opens a server connection to the backend, installs a handler for the
 * FM RFC_REMOTE_EXEC and waits for incoming requests for that FM.
 *
 * \in argc Number of arguments provided for startup.
 * \in **argv If started from the command line, the arguments must include the gateway host (-g),
 * the gateway service (-x) and the program ID under which to register (-a). Optionally they may
 * include a filename for reading access restrictions (-f) and a flag to turn on trace (-t).
 * If started from the gateway process, the arguments must consist of the connection parameters
 * required by the CPIC library.
 * \return 0 on success, 1 if anything goes wrong.
 */
int mainU(int argc, SAP_UC** argv) {
#ifdef SAPonNT
	SetConsoleCtrlHandler(shutdownHandler, TRUE);
#else
	signal(SIGINT, shutdownHandler);
	signal(SIGTERM, shutdownHandler);
	signal(SIGQUIT, shutdownHandler);
#endif

	int rc = 0;
	try {
		enableTraceIfAny(argc, argv);

		/* no SID, so any ABAP backend can call them */
		RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
		if (RfcInstallServerFunction(NULL, RFC_REMOTE_EXEC_desc::getRFC_REMOTE_EXEC_desc(), RFC_REMOTE_EXEC, &errorInfo) != RFC_OK) {
			throw RfcExecException(errorInfo.message, errorInfo.code);
		}
		if (RfcInstallServerFunction(NULL, RFC_REMOTE_EXEC_desc::getRFC_REMOTE_PIPE_desc(), RFC_REMOTE_EXEC, &errorInfo) != RFC_OK) {
			throw RfcExecException(errorInfo.message, errorInfo.code);
		}

		theServer = new RfcExecServer(argc, argv);
		Trace::get().write(cU("RfcExecServer created successfully"), NULL);
		theServer->run();
		Trace::get().write(cU("RfcExecServer is shutting down"), NULL);
	}
	catch (const RfcExecException& ex) {
		RfcExecServer::usage(ex.message);
		Trace::get().write(cU("RfcExecServer aborted with error"), ex.message);
		rc = ex.rc;
	}

	if (theServer) {
		delete theServer;
		theServer = NULL;
	}
	RFC_REMOTE_EXEC_desc::deleteAll();

	return rc;
}


/* static */ bool RfcExecServer::isStartedServer(int argc, SAP_UC** argv) {
	bool isStartedServer = false;
	if (argc > 4 &&
		(!strncmpU(argv[2], cU("sapgw"), 5) || !strncmpU(argv[2], cU("33"), 2) || !strncmpU(argv[2], cU("48"), 2)) &&
		strlenU(argv[3]) == 8 && //argument 3 is conversation ID
		isdigitU(argv[3][0]) &&
		isdigitU(argv[3][1]) &&
		isdigitU(argv[3][2]) &&
		isdigitU(argv[3][3]) &&
		isdigitU(argv[3][4]) &&
		isdigitU(argv[3][5]) &&
		isdigitU(argv[3][6]) &&
		isdigitU(argv[3][7])
		) {
		isStartedServer = true;
	}
	return isStartedServer;
}
/**
 * \brief  Constructor for our RFC Server
 * \ingroup rfcexec
 *
 * Parses the command line and then opens a server connection either in started mode or registered mode.
 * Also initializes the access restriction list, if specified.
 *
 * \in argc See mainU
 * \in **argv See mainU
 */
RfcExecServer::RfcExecServer(int argc, SAP_UC** argv) : running(true), connection(NULL), authorization(NULL) {
	RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
	SAP_UC error[256] = iU("");

	bool hasSecFile = false;
	const SAP_UC* secFilePath = NULL;
	memsetU(system, cU('\0'), sizeofU(system));

	std::vector<const SAP_UC*> additionalParams(5);
	additionalParams[0] = cU("-on_cce");
	additionalParams[1] = cU("-cfit");
	additionalParams[2] = cU("-keepalive");
	additionalParams[3] = cU("-delta");
	additionalParams[4] = cU("-no_compression");

	for (int i = 1; i < argc; ++i) {
		std::vector<const SAP_UC*>::const_iterator found_it = std::find_if(additionalParams.begin(), additionalParams.end(), StrCmp(argv[i]));

		if (found_it != additionalParams.end()) {
			RFC_CONNECTION_PARAMETER param = { NULL, NULL };
			param.name = *found_it + 1; // +1 to get rid of the "-" char

			if (i == argc - 1) {
				sprintf_sU(error, sizeofU(error), cU("Missing value for parameter %s"), argv[i]);
				throw RfcExecException(error, -1);
			}

			param.value = argv[++i];
			connectionParams.push_back(param);
		}
		else {
			if (!strcmpU(cU("-f"), argv[i])) {
				if (i == argc - 1) {
					strncpy_sU(error, sizeofU(error), cU("Missing file name"), 18);
					throw RfcExecException(error, -2);
				}

				hasSecFile = true;
				secFilePath = argv[++i];
			}
		}
	}

	if (RfcExecServer::isStartedServer(argc, argv)) {
		registered = false;

		if (!hasSecFile) {
			hasSecFile = true;
			secFilePath = cU("./rfcexec.sec");
		}

		connection = RfcStartServer(argc, argv, &connectionParams[0], connectionParams.size(), &errorInfo);
		if (connection == NULL)
			throw RfcExecException(errorInfo.message, errorInfo.code);
	}
	else { //Registered Server
		registered = true;
		unsigned flags = 0;

		for (int i = 1; i < argc; ++i) {
			RFC_CONNECTION_PARAMETER param = { NULL, NULL };
			if (!strcmpU(cU("-a"), argv[i])) {
				if ((flags & 1) == 1) {
					strncpy_sU(error, sizeofU(error), cU("Duplicate parameter \"-a\""), 25);
					throw RfcExecException(error, -3);
				}
				flags |= 1;
				param.name = cU("program_id");
			}
			else if (!strcmpU(cU("-g"), argv[i])) {
				if ((flags & 2) == 2) {
					strncpy_sU(error, sizeofU(error), cU("Duplicate parameter \"-g\""), 25);
					throw RfcExecException(error, -4);
				}
				flags |= 2;
				param.name = cU("gwhost");
			}
			else if (!strcmpU(cU("-x"), argv[i])) {
				if ((flags & 4) == 4) {
					strncpy_sU(error, sizeofU(error), cU("Duplicate parameter \"-x\""), 25);
					throw RfcExecException(error, -5);
				}
				flags |= 4;
				param.name = cU("gwserv");
			}
			else if (!strcmpU(cU("-s"), argv[i])) {
				if ((flags & 8) == 8) {
					strncpy_sU(error, sizeofU(error), cU("Duplicate parameter \"-s\""), 25);
					throw RfcExecException(error, -6);
				}
				flags |= 8;
				if (i == argc - 1) {
					strncpy_sU(error, sizeofU(error), cU("Missing system name"), 20);
					throw RfcExecException(error, -7);
				}
				strncpy_sU(system, sizeofU(system), argv[++i], 8);
				continue;
			}
			else if (!strcmpU(cU("-f"), argv[i])) { // -f has already been handled above. Don't write "Ignored connection parameter: -f"...
				++i;
				continue;
			}
			else if (std::find_if(additionalParams.begin(), additionalParams.end(), StrCmp(argv[i])) == additionalParams.end()) {
				Trace::get().write(cU("Ignored connection parameter:"), argv[i]);
				printfU(cU("Ignored connection parameter: %s\n"), argv[i]);
				continue;
			}

			if (i == argc - 1) {
				sprintf_sU(error, sizeofU(error), cU("Missing value for parameter %s"), argv[i]);
				throw RfcExecException(error, -8);
			}

			param.value = argv[++i];
			connectionParams.push_back(param);
		}

		if ((flags & 7) != 7) {
			strncpy_sU(error, sizeofU(error), cU("Not all mandatory parameters specified"), 39);
			throw RfcExecException(error, -9);
		}

		connection = RfcRegisterServer(&connectionParams[0], connectionParams.size(), &errorInfo);
		if (connection == NULL)
			throw RfcExecException(errorInfo.message, errorInfo.code);
	}

	authorization = createAuthorization(registered, hasSecFile, secFilePath, system);
	authorization->traceSecurityInfo();
}

/**
 * \brief  Destructor for our RFC Server
 * \ingroup rfcexec
 *
 * Closes the underlying connection and cleans up any occupied memory.
 */
RfcExecServer::~RfcExecServer(void) {
	if (connection) {
		RfcCloseConnection(connection, NULL);
		connection = NULL;
	}

	if (authorization) {
		delete authorization;
		authorization = NULL;
	}
}

/**
 * \brief  Prints instructions for how to start this program.
 * \ingroup rfcexec
 *
 * Optionally it prints an error message for more information on why the current start attempt failed.
 *
 * \in *param An optional error message to print before the instructions. Set to NULL, if not needed.
 */
void RfcExecServer::usage(const SAP_UC* param) {
	if (param)
		printfU(cU("Error: %s\n"), param);
	printfU(cU("\tPlease start the program in the following way:\n"));
	printfU(cU("\trfcexec -t -a <program ID> -g <gateway host> -x <gateway service>\n\t\t-f <file with list of allowed commands> -s <allowed Sys ID> RFC_TRACE=<level> CPIC_TRACE=<level>\n"));
	printfU(cU("The options \"-t\" (trace), \"-f\" and \"-s\" are optional.\n\n"));
	printfU(cU("Below further optional parameters are listed. You can find their\n"));
	printfU(cU("documentation in sapnwrfc.ini:\n"));
	printfU(cU("-on_cce <0, 1, 2> (On Character Conversion Error)\n"));
	printfU(cU("-cfit (Conversion Fault Indicator Token - the substitute symbol used if on_cce=2)\n"));
	printfU(cU("-keepalive (Sets the keepalive option. Default is 0.)\n"));
	printfU(cU("-delta <0, 1> (default 1, i.e. use delta-manager)\n"));
	printfU(cU("-no_compression (table compression, default is 0, i.e. compression is on)\n"));
}

/**
 * \brief  Main loop of the program.
 * \ingroup rfcexec
 *
 * This function waits for incoming RFC requests from the backend and dispatches them to the
 * RFC_REMOTE_EXEC function. When running in registered mode, the program loops as long as it
 * can still connect to the gateway. When running in started mode, it loops just once.
 */
void RfcExecServer::run(void) {
	RFC_RC rc = RFC_OK;
	RFC_ERROR_INFO errorInfo = RFC_ERROR_INFO();
	bool refresh = false;

	while (connection != NULL && running) {
		refresh = false;

		rc = RfcListenAndDispatch(connection, 2, &errorInfo);
		switch (rc) {
		case RFC_NOT_FOUND:
			printfU(cU("Unknown function module: %s\n"), errorInfo.message);
			Trace::get().write(cU("Unknown function module"), errorInfo.message);
			refresh = true;
			break;
		case RFC_EXTERNAL_FAILURE:
			printfU(cU("SYSTEM_FAILURE has been sent to backend: %s\n"), errorInfo.message);
			Trace::get().write(cU("SYSTEM_FAILURE has been sent to backend"), errorInfo.message);
			refresh = true;
			break;
		case RFC_ABAP_MESSAGE:
			printfU(cU("ABAP Message has been sent to backend: %s %s %s\n"), errorInfo.abapMsgType,
				errorInfo.abapMsgClass, errorInfo.abapMsgNumber);
			printfU(cU("Variables: V1=%s V2=%s V3=%s V4=%s\n"), errorInfo.abapMsgV1,
				errorInfo.abapMsgV2, errorInfo.abapMsgV3, errorInfo.abapMsgV4);
			refresh = true;
			break;
		case RFC_CLOSED:
		case RFC_INVALID_HANDLE:
		case RFC_MEMORY_INSUFFICIENT:
		case RFC_COMMUNICATION_FAILURE:
			printfU(cU("Communication Failure: %s\n"), errorInfo.message);
			Trace::get().write(cU("Communication Failure"), errorInfo.message);
			refresh = true;
			connection = NULL;
			break;
		default:
			break;
		}

		if (refresh && registered) {
			Trace::get().write(cU("Trying to reconnect..."), NULL);
			connection = RfcRegisterServer(&connectionParams[0], connectionParams.size(), &errorInfo);
			if (connection == NULL) {
				printfU(cU("Error: unable to reconnect to %s. %s:%s\n"), system, RfcGetRcAsString(errorInfo.code), errorInfo.message);
				printfU(cU("Stopping to listen at %s\n"), system);
				Trace::get().write(cU("Error: unable to reconnect"), errorInfo.message);
				Trace::get().write(cU("Stopping to listen at"), system);
			}
			else
				Trace::get().write(cU("...success"), NULL);
		}
	}
}
