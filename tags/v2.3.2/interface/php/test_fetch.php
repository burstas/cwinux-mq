<?php
        
		function test_fetch(){}
        include_once('CwxMqDef.class.php');
		function __autoload($name){
			include_once($name.".class.php");
		}
        
		$host	=	'127.0.0.1';
		$port	=	9906;
		
		$queue	=	'aa';
		$user	=	'mq_admin';
		$passwd =	'mq_admin_passwd';
		
        $poco = new CwxMqPoco();
        $request = new CwxRequest($host,$port);
        
        $pack = $poco->packFetchMq($queue,null,$user,$passwd);
                 
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
       	
        $r = $poco->parserReply($ret);
        if($r === false){
        	echo $poco->getLastError();
        	exit;
		}
		
		echo "<pre>";
		print_r($r);
		
        
?>
