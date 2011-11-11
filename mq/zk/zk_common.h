#ifndef __ZK_COMMON_H__
#define __ZK_COMMON_H__

#include "ZkJPoolAdaptor.h"
#include <getopt.h>
#include <errno.h>
#include <dirent.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <fcntl.h>

void output(FILE* fd, int result, int zkstate, char const* format, char const* msg);

bool readFile(string const& strFileName, string& strData);

class ZkGetOpt
{
public:
	enum {
		/// Doesn't take an argument.
		NO_ARG = 0,
		/// Requires an argument, same as passing ":" after a short option
		/// character in optstring.
		ARG_REQUIRED = 1,
		/// Argument is optional, same as passing "::" after a short
		/// option character in optstring.
		ARG_OPTIONAL = 2
	};
	///构造函数
	ZkGetOpt (int argc,
		char **argv,
		char const* optstring = "");
	///析构函数
	~ZkGetOpt (void);
public:
	///设置long option对应的short option
	int long_option (char const *name, int short_option, int has_arg = NO_ARG);
	/**
	@brief 获取下一个参数
	@return 与getopt_long()相同
	*/
	int next();
	///获取option的参数
	char *opt_arg (void) const;
	///返回当前的option
	int opt_opt (void) const;
	///返回当前参数的index
	int opt_ind (void) const;
	///返回当前的long option名
	char const* long_option() const;
private:
	int             m_argc;
	char **         m_argv;
	char const*     m_optString;
	struct option*   m_longOptions;
	int             m_longindex;
	int             m_longOptionsNum;
	int             m_longOptionsSize;
	int             m_opt;
};

#endif 
