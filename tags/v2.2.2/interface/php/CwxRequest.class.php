<?php 

/**
 * 封装请求过程
 * 
 * todo:判断连接是否存在的方法过于简单
 * todo:是否应该使用Listen?
 *
 */

class CwxRequest
{
	/**
	 * 服务器HOST
	 *
	 * @var string
	 */
    private $host;
    
    /**
     * 服务器端口
     *
     * @var integer
     */
    private $port;
    
    /**
     * socket连接
     *
     * @var resource 
     */
    private $sock;
    
    /**
     * 构造函数
     *
     * @param string $host
     * @param integer $port
     */
    public function __construct($host,$port)
    {
        $this->host = $host;
        $this->port = $port;
    }
    
    /**
     * 错误消息
     *
     * @var string
     */
    private $error;
    /**
     * 错误代码
     *
     * @var integer
     */
    private $errno;

    /**
     * 建立socket链接
     *
     * @return boolean
     */
    private function connect()
    {
    	//应该用更合适的方法去判断连接是否已经建立
    	if($this->sock == true){
    		return true;
    	}
    	$commonProtocol = getprotobyname("tcp");
        $socket = socket_create(AF_INET,SOCK_STREAM,$commonProtocol);
        
        if(socket_connect($socket,$this->host,$this->port)==false){
        	$this->errno = -1;
        	$this->error = "建立连接失败[{$this->host}:{$this->port}]";
        	return false;
        }
        $this->sock = $socket;
        return true;
    }
    
    /**
     * 发送请求，并返回获得的消息体
     *
     * @param unknown_type $package
     * @return unknown
     */
    public function request($package)
    {   
        $ret =	$this->connect();
        if($ret == false){
        	return false;
        }
        
        $socket = $this->sock;
        $data = socket_write($socket,$package);

        $rdata = socket_read($socket,14,PHP_BINARY_READ );       
        $n = strlen($rdata);
        if($n == 14){
        	$header = new CwxMsgHead();
        	$ret = $header->fromNet($rdata);        	
        	if($ret == true){
        		
        		$dataLen = $header->getDataLen(); 
        		
        		$rdata = null;
        		$n = 0;
        		
        		while( $n < $dataLen){
        			$rt = socket_read($socket,512,PHP_BINARY_READ);
        			$rdata .= $rt;
        			$n = $n+strlen($rt); 
        		}
        		
        		if($n == $dataLen){
        			//处理压缩的消息，进行解压缩操作
        			if( ($header->getAttr() & 2) == true){
        				$rdata = gzuncompress($rdata);
        			}
        			return $rdata;
        		}
        		else{
        			$this->errno = -1;
        			$this->error = '获取消息体失败 返回数据长度错误';
        			return false;	
        		}
        	}
        	else{
        		$this->errno = -1;
        		$this->error = $header->getLastError();
        		return false;	
        	}
        }
        else{
        	$this->errno = -1;
        	$this->error = '获取消息头失败 返回数据长度错误';
        	return false;
        }
    }
    
    
    public function getSocket()
    {
        $commonProtocol = getprotobyname("tcp");
        $socket = socket_create(AF_INET,SOCK_STREAM,$commonProtocol);
        
        if(socket_connect($socket,$this->host,$this->port)==false){
        	$this->errno = -1;
        	$this->error = "建立连接失败[{$this->host}:{$this->port}]";
        	return false;
        }
        return $socket;
    }
    /**
     * 发送消息体
     *
     * @param unknown_type $package
     * @return unknown
     */
    public function sendMsg($socket,$package)
    {
        if(strlen($package) == 0){
            $this->errno = -1;
            $this->error = '消息体不能为空串';
            return false;
        }
        
        $data = socket_write($socket,$package);
        if($data === false){
            $this->errno = -1;
            $this->error = '发送消息失败，请检查连接是否正常';
            return false;
        }
        else{
            return true;
        }
    }
    
    /**
     * 接收消息体
     *
     * @param unknown_type $package
     * @return unknown
     */
    public function receiveMsg($socket)
    {   
        $rdata = socket_read($socket,14,PHP_BINARY_READ );       
        $n = strlen($rdata);
        if($n == 14){
        	$header = new CwxMsgHead();
        	$ret = $header->fromNet($rdata);        	
        	if($ret == true){
        		
        		$dataLen = $header->getDataLen(); 
        		
        		$rdata = null;
        		$n = 0;
        		
        		while( $n < $dataLen){
        			$rt = socket_read($socket,512,PHP_BINARY_READ);
        			$rdata .= $rt;
        			$n = $n+strlen($rt); 
        		}
        		
        		if($n == $dataLen){
        			//处理压缩的消息，进行解压缩操作
        			if( ($header->getAttr() & 2) == true){
        				$rdata = gzuncompress($rdata);
        			}
        			return $rdata;
        		}
        		else{
        			$this->errno = -1;
        			$this->error = '获取消息体失败 返回数据长度错误';
        			return false;	
        		}
        	}
        	else{
        		$this->errno = -1;
        		$this->error = $header->getLastError();
        		return false;	
        	}
        }
        else{
        	$this->errno = -1;
        	$this->error = '获取消息头失败 返回数据长度错误';
        	return false;
        }
    }
      /**
     * 获取最后的错误信息
     *
     * @return string
     */
    public function getLastError(){
    	return $this->error;
    }
    
    /**
     * 获取最后的错误代码
     *
     * @return integer
     */
    public function getLastErrno(){
    	return $this->errno;
    }
    
    /*
    public function request2($package)
    {   
        $ret =	$this->connect();
        
        if($ret == false){
        	return false;
        }
        
        $socket = $this->sock;
        $data = socket_write($socket,$package);

        $n = socket_recv($socket,$rdata,512,PHP_BINARY_READ );
        
        var_dump($n);
        if ( $n <= 0 )	{
            $rt['errcode']=-3;
            return $rt;
        }
        //socket_close($socket);        
        return $rdata;
    }*/

}


?>
