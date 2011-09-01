<?php
		function test_send(){}
        include_once('CwxMqDef.class.php');
		function __autoload($name){
			include_once($name.".class.php");
		}

		$host	=	'127.0.0.1';
		$port	=	9901;
		
		$group	=	'3';
		$type	=	'5';
		$user	=	'recv';
		$passwd =	'recv_passwd';
		
		
		//这个还没有测试通过
		$sign	=	'crc32';		
		$zip	=	1;
		
		$poco = new CwxMqPoco();
        $request = new CwxRequest($host,$port);
        
        $data = 'msg '.date('Y-m-d H:i:s ').rand(100,999);
                
        $pack = $poco->packRecvData(0,$data,$group,$type,$user,$passwd,$sign,$zip);
        
        $ret = $request->connect();
        if($ret === false){
       		echo $request->getLastError();
       		exit;
       	}
       	
        $ret = $request->sendMsg($pack);
        if($ret === false){
       		echo $request->getLastError();
       		exit;
       	}
       	
        $ret = $request->receiveMsg();
        if($ret === false){
       		echo $request->getLastError();
       		exit;
       	}
       	
        $request->close();
        
        $r = $poco->parserReply($ret);
        if($r === false){
        	echo $poco->getLastError();
        	exit;
		}
		
		echo "<pre>";
		print_r($r);
	
        
?>
