<?php
		function test_send(){}
        include_once('CwxMqDef.class.php');
		function __autoload($name){
			include_once($name.".class.php");
		}

		$host	=	'123.125.104.62';
		$port	=	9901;
		
		$group	=	'3';
		$type	=	'5';
		$user	=	'recv';
		$passwd =	'recv_passwd';
		
		//�������ô���£�
		$attr	=	null;
		
		//�����û�в���ͨ��
		$sign	=	'crc32';
		
		$zip	=	1;
		
		$poco = new CwxMqPoco();
        $request = new CwxRequest($host,$port);
        
        $data = 'msg '.date('Y-m-d H:i:s ').rand(100,999);
                
        $pack = $poco->packRecvData(0,$data,$group,$type,$attr,$user,$passwd,$sign,$zip);
         
        $ret = $request->request($pack);        
       	if($ret === false){
       		echo $request->getLastError();
       		exit;
       	}
        $r = $poco->parserReply($ret);
        if($r === false){
        	echo $poco->getLastError();
        	exit;
		}
		
		echo "<pre>";
		print_r($r);
		
		/*
		flush();
		sleep(1);
		
		$request = new CwxRequest('123.125.104.62',9901);
		$data = 'msg '.date('Y-m-d H:i:s ').rand();
        $pack = $poco->packRecvData(0,$data,'a','b',null,'recv','recv_passwd');
         
        $ret = $request->request($pack);        
       	if($ret === false){
       		echo $request->getLastError();
       		exit;
       	}
        $r = $poco->parserReply($ret);
        if($r === false){
        	echo $poco->getLastError();
        	exit;
		}
		
		echo "<pre>";
		print_r($r);
		*/
        
?>