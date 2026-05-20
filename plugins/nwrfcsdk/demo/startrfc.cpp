#include "startrfc.h"

int mainU (int argc, SAP_UC ** argv)
{
    OPTIONS options = {};
    if(!parseCommand(argc, argv, &options))
        return 0;
    if(!checkOptions(&options))
    {
        showHelp();
        return 1;
    }
    const RFC_RC rc = startRfc(&options);
    return rc;
}

bool parseCommand(int argc, SAP_UC** argv, OPTIONS* options)
{
    const SAP_UC* const PATHNAME = cU("PATHNAME=");
    const SAP_UC* const PORT = cU("PORT=");
    const size_t PATHNAME_LEN = 9;
    const size_t PORT_LEN = 5;

    if (argc < 2 || !strcmpU(argv[1], cU("-help")) || !strcmpU(argv[1], cU("-?")))
    {
        showHelp();
        return false;
    }
    else if (!strcmpU(argv[1], cU("-v")))
    {
        showVersion();
        return false;
    }

    for (int i = 1; i < argc; i++)
    {
        const SAP_UC* const arg = argv[i];
        if (strcmpU(arg, cU("-i")) == 0)
        {
            options->showSysInfo = true;
        }
        else if (strcmpU(arg, cU("-h")) == 0)
        {
            if (++i < argc)
                options->ashost = argv[i];
            else
                printfU(cU("Missing parameter after option -h\n"));
        }
        else if (strcmpU(arg, cU("-s")) == 0)
        {
            if (++i < argc)
                options->sysnr = argv[i];
            else
                printfU(cU("Missing parameter after option -s\n"));
        }
        else if (strcmpU(arg, cU("-u")) == 0)
        {
            if (++i < argc)
                options->user = argv[i];
            else
                printfU(cU("Missing parameter after option -u\n"));
        }
        else if (strcmpU(arg, cU("-p")) == 0)
        {
            if (++i < argc)
                options->passwd = argv[i];
            else
                printfU(cU("Missing parameter after option -p\n"));
        }
        else if (strcmpU(arg, cU("-c")) == 0)
        {
            if (++i < argc)
                options->client = argv[i];
            else
                printfU(cU("Missing parameter after option -c\n"));
        }
        else if (strcmpU(arg, cU("-l")) == 0)
        {
            if (++i < argc)
                options->language = argv[i];
            else
                printfU(cU("Missing parameter after option -l\n"));
        }
        else if (strcmpU(arg, cU("-D")) == 0)
        {
            if (++i < argc)
                options->dest = argv[i];
            else
                printfU(cU("Missing parameter after option -D\n"));
        }
        else if (strcmpU(arg, cU("-F")) == 0)
        {
            if (++i < argc)
                options->function = argv[i];
            else
                printfU(cU("Missing parameter after option -F\n"));
        }
        else if (strcmpU(arg, cU("-E")) == 0)
        {
            if (++i < argc)
            {
                const SAP_UC* const param = argv[i];
                if (!strncmpU(param, PATHNAME, PATHNAME_LEN))
                {
                    options->path = param + PATHNAME_LEN;
                }
                else if (!strncmpU(param, PORT, PORT_LEN))
                {
                    options->port = param + PORT_LEN;
                }
            }
            else
                printfU(cU("Missing parameter after option -E\n"));
        }
        else if (strcmpU(arg, cU("-t")) == 0)
        {
            if (++i < argc)
            {
                const unsigned traceLevel = atoiU(argv[i]);
                RfcSetTraceLevel(NULL, NULL, traceLevel, NULL);
            }
            else
                printfU(cU("Missing parameter after option -t\n"));
        }
        else
        {
            printfU(cU("Unknown command option %s\n"), arg);
        }
    }

    return true;
}

bool checkOptions(OPTIONS *options)
{
    const SAP_UC * const EDI_DATA_INCOMING = cU("EDI_DATA_INCOMING");
    const SAP_UC * const EDI_STATUS_INCOMING = cU("EDI_STATUS_INCOMING");
    const unsigned MAX_PATH_LEN = 100;
    const unsigned MAX_PORT_LEN = 10;

    if(!options->showSysInfo)
    {
        if((!options->function) ||
            (strcmpU(options->function,EDI_DATA_INCOMING) && 
            strcmpU(options->function,EDI_STATUS_INCOMING)))
        {
            printfU(cU("Missing or invalid -F option.\n"));
            return false;
        }

        if(!options->path || !options->path[0])
        {
            printfU(cU("Missing or invalid -E PATHNAME=  option.\n"));
            return false;
        }
        else if(strlenU(options->path) > MAX_PATH_LEN)
        {
            printfU(cU("Path specified by -E PATHNAME= excceeds the maximum length of 100. \n"));
            return false;
        }
        if(!options->port ||!options->port[0] )
        {
            printfU(cU("Missing or invalid -E PORT=  option.\n"));
            return false;
        }
        else if(strlenU(options->port) > MAX_PORT_LEN)
        {
            printfU(cU("Port name specified by -E PORT= excceeds the maximum length of 10. \n"));
            return false;
        }
   }
   return true;
}

RFC_RC startRfc(OPTIONS* options)
{
    RFC_RC rc = RFC_OK;
    RFC_ERROR_INFO error = RFC_ERROR_INFO();
    RFC_CONNECTION_HANDLE connHandle = NULL;
    RFC_FUNCTION_DESC_HANDLE funcDesc = NULL;
    RFC_FUNCTION_HANDLE funcHandle = NULL;

    const RFC_CONNECTION_PARAMETER connParams[] = {
                                {cU("ashost"), options->ashost},
                                {cU("sysnr"), options->sysnr},
                                {cU("client"), options->client},
                                {cU("lang"), options->language ? options->language : cU("E")},
                                {cU("user"), options->user},
                                {cU("passwd"), options->passwd},
                                {cU("dest"), options->dest ? options->dest : cU("")} };

    try
    {
        connHandle = RfcOpenConnection(connParams, sizeofR(connParams) / sizeofR(RFC_CONNECTION_PARAMETER), &error);
        if (!connHandle)
            throw error;

        if (options->showSysInfo)
        {
            RFC_ATTRIBUTES attr = RFC_ATTRIBUTES();
            rc = RfcGetConnectionAttributes(connHandle, &attr, &error);
            if (rc != RFC_OK)
                throw error;
            showConnAttr(&attr);
        }
        else if (options->function)
        {

            funcDesc = getFunctionDescHandle(options->function, &error);
            if (!funcDesc)
                throw error;

            funcHandle = RfcCreateFunction(funcDesc, &error);
            if (!funcHandle)
                throw error;

            rc = RfcSetChars(funcHandle, cU("PATHNAME"), options->path, (unsigned)strlenU(options->path), &error);
            if (rc != RFC_OK)
                throw error;

            rc = RfcSetChars(funcHandle, cU("PORT"), options->port, (unsigned)strlenU(options->port), &error);
            if (rc != RFC_OK)
                throw error;

            rc = RfcInvoke(connHandle, funcHandle, &error);
            if (rc != RFC_OK)
                throw error;

        }
    }
    catch (const RFC_ERROR_INFO& info)
    {
        printfU(cU("Error: %s:%s\n"), RfcGetRcAsString(info.code), info.message);
        rc = info.code;
    }

    close(connHandle, funcDesc, funcHandle);

    return rc;
}

void close(RFC_CONNECTION_HANDLE connHandle, RFC_FUNCTION_DESC_HANDLE funcDescHandle, RFC_FUNCTION_HANDLE funcHandle)
{
    if (connHandle)
    {
        RfcCloseConnection(connHandle, NULL);
        connHandle = NULL;
    }

    if(funcHandle)
    {
        RfcDestroyFunction(funcHandle, NULL);
        funcHandle = NULL;
    }
    
    if (funcDescHandle)
    {
        RfcDestroyFunctionDesc(funcDescHandle, NULL);
        funcDescHandle = NULL;
    }
}

RFC_FUNCTION_DESC_HANDLE getFunctionDescHandle(const SAP_UC* functionName, RFC_ERROR_INFO* error)
{
    const RFC_PARAMETER_DESC parDescPathname = { iU("PATHNAME"), RFCTYPE_CHAR, RFC_IMPORT, 100, 200, 0, 0, 0, 0, 0 };
    const RFC_PARAMETER_DESC parDescPort =     { iU("PORT"),     RFCTYPE_CHAR, RFC_IMPORT,  10,  20, 0, 0, 0, 0, 0 };

    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcCreateFunctionDesc(functionName, error);
    if (!funcDesc)
        return NULL;

    RFC_RC rc = RfcAddParameter(funcDesc, &parDescPathname, error);
    if (rc != RFC_OK)
    {
        RfcDestroyFunctionDesc(funcDesc, NULL);
        return NULL;
    }

    rc = RfcAddParameter(funcDesc, &parDescPort, error);
    if (rc != RFC_OK)
    {
        RfcDestroyFunctionDesc(funcDesc, NULL);
        return NULL;
    }
    return funcDesc;
}


void showHelp(void)
{
    const SAP_UC * const programName = cU("startrfc");
    printfU( cU("\nUsage: %s [options]\n"), programName );
    printfU( cU("Options:\n") );
    printfU( cU("  -h <ashost>          SAP application server to connect to\n") );
    printfU( cU("  -s <sysnr>           system number of the target SAP system\n") );
    printfU( cU("  -u <user>            user\n") );
    printfU( cU("  -p <passwd>          password\n") );
    printfU( cU("  -c <client>          client \n") );
    printfU( cU("  -l <language>        logon language\n") );
    printfU( cU("  -D <destination>     destination defined in RFC config file sapnwrfc.ini\n") );
    printfU( cU("  -F <function>        function module to be called, only EDI_DATA_INCOMING\n") );
    printfU( cU("                       or EDI_STATUS_INCOMING is supported\n") );
    printfU( cU("  -E PATHNAME=<path>   path, including file name, to EDI data file or status \n") );
    printfU( cU("                       file, with maximum length of 100 charachters\n") );
    printfU( cU("  -E PORT=<port name>  port name of the ALE/EDI interface with maximum   \n") );
    printfU( cU("                       length of 10 charachters\n") );
    printfU( cU("  -t <level>           set global RFC tracelevel 0(off), 1(brief), 2(verbose), 3(detailed) or 4(full)\n") );
    printfU( cU("  -help  or -?         display this help page\n") );
    printfU( cU("  -v                   display the version of the NWRFC library, the version\n") );
    printfU( cU("                       of the compiler used by SAP to build this program and\n") );
	printfU( cU("                       the version of startrfc\n") );
    printfU( cU("  -i                   connect to the target system and display the system info\n") );
}

void showConnAttr(RFC_ATTRIBUTES* attr)
{
    if (!attr)
        return;
    printfU(cU("SAP System ID: %s\n"), attr->sysId);
    printfU(cU("SAP System Number: %s\n"), attr->sysNumber);
    printfU(cU("Partner Host: %s\n"), attr->partnerHost);
    printfU(cU("Own Host: %s\n"), attr->host);
    printfU(cU("Partner System Release: %s\n"), attr->partnerRel);
    printfU(cU("Partner Kernel Release: %s\n"), attr->kernelRel);
    printfU(cU("Own Release: %s\n"), attr->rel);
    printfU(cU("Partner Codepage: %s\n"), attr->partnerCodepage);
    printfU(cU("Own Codepage: %s\n"), attr->codepage);
    printfU(cU("User: %s\n"), attr->user);
    printfU(cU("Client: %s\n"), attr->client);
    printfU(cU("Language: %s\n"), attr->language);
}



void showVersion(void)
{
    printfU (cU("NW RFC Library Version: %s\n"), RfcGetVersion(NULL, NULL, NULL));
    printfU (cU("Compiler Version:\n")
#if defined SAPonAIX
	#if defined(__clang__)
		cU("%s\n"), cU(__VERSION__)
    #else	
		cU("%04X (VVRR)\n"), __xlC__
	#endif
#elif defined SAPonHP_UX
		cU("%06d (VVRRPP. %s Compiler)\n"),	/*yes, decimal here!*/
	#if defined __HP_cc
			__HP_cc, cU("C")
	#elif defined __HP_aCC
			__HP_aCC, cU("C++")
	#else
			0, cU("Unknown Version")
	#endif
#elif defined SAPonLINUX
		cU("%s\n"), cU(__VERSION__)
#elif defined SAPonNT
		cU("%09d (VVRRPPPPP. Microsoft (R) C/C++ Compiler)\n"), _MSC_FULL_VER		/*decimal!*/
#elif defined SAPonSUN
		cU("%03X (VRP. %s Compiler)\n"),
	#if defined __SUNPRO_C
			__SUNPRO_C, cU("C")
	#elif defined __SUNPRO_CC
			__SUNPRO_CC, cU("C++")
	#else
			0, cU("Unknown Version")
	#endif
#elif defined SAPonOS390
        cU("%08X (PVRRMMMM)\n"), __COMPILER_VER__
#else
		cU("%s\n"), cU("Version not available.")
#endif
	);
    printfU (cU("Startrfc Version: 2024-01-20\n"));
}

