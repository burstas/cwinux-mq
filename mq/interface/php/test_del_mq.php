<?php
        function test_del_mq(){}
        include_once('CwxMqDef.class.php');
		function __autoload($name){
			include_once($name.".class.php");
		}
		
        $host	=	'127.0.0.1';
		$port	=	9906;
		
		$queue	=	'aa';
		$user	=	'mq_admin';
		$passwd =	'mq_admin_passwd';
		
		$auth_user		=	'mq_admin';
		$auth_passwd 	=	'mq_admin_passwd';
		
        $poco = new CwxMqPoco();
        $request = new CwxRequest($host,$port);
        
        $pack = $poco->packDelQueue($queue,$user,$passwd,$auth_user,$auth_passwd);        
        
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
        
?>
