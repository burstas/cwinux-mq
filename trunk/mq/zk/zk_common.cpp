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
