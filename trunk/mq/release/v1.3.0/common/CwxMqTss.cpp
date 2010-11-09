#include "CwxMqTss.h"

///¹¹Ôìº¯Êý
CwxMqTss::~CwxMqTss()
{
    if (m_pReader) delete m_pReader;
    if (m_pWriter) delete m_pWriter;
    if (m_szDataBuf) delete []m_szDataBuf;
}

int CwxMqTss::init()
{
    m_pReader = new CwxPackageReader(false);
    m_pWriter = new CwxPackageWriter(MAX_PACKAGE_SIZE);
    m_szDataBuf = new char[MAX_PACKAGE_SIZE];
    m_uiDataBufLen= MAX_PACKAGE_SIZE;
    return 0;
}
