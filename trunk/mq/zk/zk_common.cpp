#include "zk_common.h"
void output(FILE* fd, int result, int zkstate, char const* format, char const* msg)
{
	if (fd)
	{
		fprintf(fd, "ret:  %d\n", result);
		fprintf(fd, "zkstate:  %d\n", zkstate);
		if (format)
			fprintf(fd, format, msg);
		else
			fprintf(fd, msg);
	}
	else
	{
		printf("ret:  %d\n", result);
		printf("zkstate:  %d\n", zkstate);
		if (format)
			printf(format, msg);
		else
			printf(msg);
	}
}

bool readFile(string const& strFileName, string& strData)
{
	struct stat info;
	stat(strFileName.c_str(), &info);
	off_t offset = info.st_size;
	if (-1  == offset) return false;
	size_t file_size = offset;
	FILE* fd =fopen(strFileName.c_str(), "rb");
	char* szBuf = new char[file_size + 1];
	memset(szBuf, 0x00, file_size + 1);
	if (file_size != fread(szBuf, 1, file_size, fd))
	{
		fclose(fd);
		delete [] szBuf;
		return false;
	}
	strData.assign(szBuf, file_size);
	fclose(fd);
	delete [] szBuf;
	return true;

}


///构造函数
ZkGetOpt::ZkGetOpt (int argc,
					  char **argv,
					  char const* optstring)
{
	struct option  empty={0,0,0,0};
	m_argc = argc;
	m_argv = argv;
	m_optString = optstring;
	m_longOptionsNum = 0;
	m_longOptionsSize = 16;
	m_longOptions = new option[m_longOptionsSize];
	for (int i=0; i<m_longOptionsSize; i++)
	{
		memcpy(&m_longOptions[i], &empty, sizeof empty);
	}
	m_opt = 0;
	m_longindex = 0;
	optarg = 0;
	optopt = 0;
	opterr   =  0;
	optind   =  1;

}

ZkGetOpt::~ZkGetOpt (void)
{
	if (m_longOptions)
	{
		for (int i=0; i<m_longOptionsNum; i++)
		{
			if(m_longOptions[i].name) delete m_longOptions[i].name;
		}
		delete [] m_longOptions;
		m_longOptions = NULL;
	}
}

int ZkGetOpt::long_option (char const *name, int short_option, int has_arg)
{
	int i;
	if (m_longOptionsNum + 2 >= m_longOptionsSize)
	{
		m_longOptionsSize *= 2;
		struct option* tmp = new option[m_longOptionsSize];
		for (i=0; i<m_longOptionsNum; i++)
		{
			memcpy(&tmp[i], &m_longOptions[i], sizeof m_longOptions[i]);
		}
		struct option  empty={0,0,0,0};
		for (; i<m_longOptionsSize; i++)
		{
			memcpy(&m_longOptions[i], &empty, sizeof empty);
		}
	}
	char* szName = (char*)malloc(strlen(name) + 1);
	m_longOptions[m_longOptionsNum].name = szName;
	strcpy(szName, name);
	m_longOptions[m_longOptionsNum].has_arg = has_arg;
	m_longOptions[m_longOptionsNum].flag = NULL;
	m_longOptions[m_longOptionsNum].val = short_option;
	m_longOptionsNum++;
	return 0;
}

int ZkGetOpt::next()
{
	m_longindex = 0;
	m_opt = getopt_long (m_argc, m_argv, m_optString, m_longOptions, &m_longindex);
	return m_opt;
}

///获取option的参数
char* ZkGetOpt::opt_arg (void) const
{
	return optarg;
}
///返回当前的option
int ZkGetOpt::opt_opt (void) const
{   
	return m_opt;
}
///返回当前参数的index
int ZkGetOpt::opt_ind (void) const
{
	return optind;
}
///返回当前的long option名
char const* ZkGetOpt::long_option() const
{
	if (m_longindex < m_longOptionsNum) return m_longOptions[m_longindex].name;
	return NULL;
}
