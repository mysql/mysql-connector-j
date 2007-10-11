/*
      Copyright (C) 2005 MySQL AB

      This program is free software; you can redistribute it and/or modify
      it under the terms of version 2 of the GNU General Public License as 
      published by the Free Software Foundation.

      There are special exceptions to the terms and conditions of the GPL 
      as it is applied to this software. View the full text of the 
      exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
      software distribution.

      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.

      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.mysql.jdbc.trace;

import java.io.PrintStream;
import java.sql.SQLException;

import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.log.StandardLogger;
import com.mysql.jdbc.Connection;

import org.aspectj.lang.JoinPoint;

public aspect Tracer {

    pointcut constructors(): execution(* *(..)) && within(com.mysql.jdbc.* ) 
    	&& within (!com.mysql.jdbc.trace.*) && within(!com.mysql.jdbc.log.*)
    	&& within (!com.mysql.jdbc.Util);
  
    pointcut methods(): execution(* *(..)) && within(com.mysql.jdbc.* ) 
    	&& within(!com.mysql.jdbc.trace.*) && within(!com.mysql.jdbc.log.*)
    	&& within (!com.mysql.jdbc.Util);

    before(): constructors() && methods() {
		entry(thisJoinPoint, false);
    }
    
    after() returning (Object o): constructors() && methods() {
	  exit(thisJoinPoint, false, o);
    }

	private Log standardLogger = new StandardLogger("MySQL", false);
	
    private ThreadLocal stream = new ThreadLocal() {
	    protected Object initialValue() {
			return System.err;
	    }
	};
	
	private ThreadLocal log = new ThreadLocal() {
		protected Object initialValue() {
			return standardLogger;
	    }
	};
	
    private ThreadLocal callDepth = new ThreadLocal() {
	    protected Object initialValue() {
			return new Integer(0);
	    }
	};

    private PrintStream getStream() { 
		return (PrintStream)stream.get(); 
    }
    
    private void setStream(PrintStream s) { 
		stream.set(s); 
    }
    
    private int  getCallDepth() { 
		return ((Integer)(callDepth.get())).intValue();
    }
    
    private void setCallDepth(int n) { 
		callDepth.set(new Integer(n)); 
    }
    
    private Log getLog() { 
		return (Log)log.get();
    }
    
    private void setLog(Log l) { 
		log.set(l); 
    }

    private void entry(JoinPoint jp, boolean isConstructor) {
		
		if (jp.getTarget() instanceof com.mysql.jdbc.Connection) {
			if ("getLog".equals(jp.getSignature().getName())) {
				return;
			}
			
    		try {
    			Log connectionLog = ((com.mysql.jdbc.Connection)jp.getTarget()).getLog();

    			if (getLog() != connectionLog) {
    				setLog(connectionLog);
	    		}
	    	} catch (SQLException ex) {
	    		// swallow it, can't do anything here
	    	}
    	}
    	
    	if ("com.mysql.jdbc.Buffer".equals(jp.getSignature().getDeclaringTypeName())
    		&& ("toString".equals(jp.getSignature().getName())
    		 || "dumpClampedBytes".equals(jp.getSignature().getName()))) {
    		return;
    	}
    	
    	if ("com.mysql.jdbc.StringUtils".equals(jp.getSignature().getDeclaringTypeName())
    		&& "dumpAsHex".equals(jp.getSignature().getName())) {
    		return;
    	}
    	
		setCallDepth(getCallDepth() + 1);
		printEntering(jp, isConstructor);
    }

    private void exit(JoinPoint jp,  boolean isConstructor, Object returnValue) {
    	if (jp.getTarget() instanceof com.mysql.jdbc.Connection) {
			if ("getLog".equals(jp.getSignature().getName())) {
				return;
			}
		}
		
    	if ("com.mysql.jdbc.Buffer".equals(jp.getSignature().getDeclaringTypeName())
    		&& ("toString".equals(jp.getSignature().getName())
    		 || "dumpClampedBytes".equals(jp.getSignature().getName()))) {
    		return;
    	}
    	
    	if ("com.mysql.jdbc.StringUtils".equals(jp.getSignature().getDeclaringTypeName())
    		&& "dumpAsHex".equals(jp.getSignature().getName())) {
    		return;
    	}
			
		printExiting(jp, isConstructor, returnValue);
		setCallDepth(getCallDepth() - 1);
    }

    private void printEntering (JoinPoint jp, boolean isConstructor) {
    	
    	
    	if (getLog().isTraceEnabled()) {
    		
    		StringBuffer buf = new StringBuffer(80);
			printIndent(buf);
			buf.append("--> ");

			buf.append(jp.getSourceLocation().getFileName());
			buf.append(":");
			buf.append(jp.getSourceLocation().getLine());
			buf.append(" ");
			buf.append(jp.getSignature().getDeclaringTypeName());
			buf.append(".");
			buf.append(jp.getSignature().getName());
    		printParameters(jp, buf);

			getLog().logTrace(buf);
		}
    }

    private void printExiting (JoinPoint jp, boolean isConstructor, Object returnValue) {
    	if (getLog().isTraceEnabled()) {
    		StringBuffer buf = new StringBuffer(80);
			printIndent(buf);
			
			buf.append("<--  ");
			buf.append(jp.getSourceLocation().getFileName());
			buf.append(":");
			buf.append(jp.getSourceLocation().getLine());
			buf.append(" ");
			buf.append(jp.getSignature().getDeclaringTypeName());
			buf.append(".");
			buf.append(jp.getSignature().getName());
			buf.append("(..) returning ");
			
			boolean isString = returnValue instanceof String;
  			
  			if (isString) {
  				buf.append("\"");
  			}
  	    	
  	    	buf.append(returnValue);
  	    	
  	    	if (isString) {
  				buf.append("\"");
  			}

			getLog().logTrace(buf);
		}
    }



    private void printIndent(StringBuffer buf) {
		for (int i = 0; i < getCallDepth(); i++) {
	    	buf.append(" ");
	    }
    }
    
    private void printParameters(JoinPoint jp, StringBuffer buf) {
  		Object[] params = jp.getArgs();
	 	
  		buf.append("(");
  		
  		for (int i = 0; i < params.length; i++) {
  			boolean isString = params[i] instanceof String;
  			
  			if (isString) {
  				buf.append("\"");
  			}
  	    	
  	    	if (params[i] != null) {
  	    		Class paramClass = params[i].getClass();
  	    		String paramClassName = null;
  	    		
  	    		if (paramClass != null) {
  	    			paramClassName = paramClass.getName();
  	    		}
  	    		
  	    		if (paramClassName!= null &&
  	    			"com.mysql.jdbc.Buffer".equals(paramClassName)
  	    			|| "com.mysql.jdbc.ByteArrayBuffer".equals(paramClassName)
  	    			|| "com.mysql.jdbc.ChannelBuffer".equals(paramClassName)) {
  	    			buf.append("Network packet, data follows:\n\n");
  	    			buf.append(params[i]);
  	    			buf.append("\n\n");
  	    		} else {
  	    			buf.append(params[i]);
  	    		}
  	    	} else {
  	    		buf.append("null");
  	    	}
  	    	
  	    	if (isString) {
  				buf.append("\"");
  			}
  			
  	    	if (i < params.length - 1) {
  	    		buf.append(", ");
  	    	}
  		}
  		
  		buf.append(")");
    }

}

