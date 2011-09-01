<?php
        
		function test_report(){}		
        include_once('CwxMqDef.class.php');
		function __autoload($name){
			include_once($name.".class.php");
		}
		
        $host	=	'127.0.0.1';
		$port	=	9903;
		
		$sid = null;
		$user	=	'async';
		$passwd =	'async_passwd';
		
		$subscribe = null;
		$sign = null;
        $zip = 0;
        
        $chunkSize = 0;
        
        $poco = new CwxMqPoco();
        $request = new CwxRequest($host,$port);
        
        $pack = $poco->packReportData(0,$sid,$chunkSize,$window,$subscribe,$user,$passwd,$sign,$zip);         
        
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
		
        
        while($r == true){
        	
        	$sid = $r['sid'];
        	
        	ob_flush();
        	flush();
        	
        	if($sid > 0){

        		sleep(1);                
        		$pack = $poco->packReportDataReply(0,$sid);
        		
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
				echo date("Y-m-d H:i:s\n");
        		print_r($r);

        	}
        }
        
        
?>
