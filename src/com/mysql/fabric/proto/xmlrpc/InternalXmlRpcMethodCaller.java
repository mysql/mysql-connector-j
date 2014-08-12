/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.fabric.proto.xmlrpc;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mysql.fabric.FabricCommunicationException;
import com.mysql.fabric.xmlrpc.Client;
import com.mysql.fabric.xmlrpc.base.Array;
import com.mysql.fabric.xmlrpc.base.Member;
import com.mysql.fabric.xmlrpc.base.MethodCall;
import com.mysql.fabric.xmlrpc.base.MethodResponse;
import com.mysql.fabric.xmlrpc.base.Param;
import com.mysql.fabric.xmlrpc.base.Params;
import com.mysql.fabric.xmlrpc.base.Struct;
import com.mysql.fabric.xmlrpc.base.Value;

/**
 * An XML-RPC method caller which uses the internal XML-RPC client
 * library.
 */
public class InternalXmlRpcMethodCaller implements XmlRpcMethodCaller {
    private Client xmlRpcClient;

    public InternalXmlRpcMethodCaller(String url) throws FabricCommunicationException {
        try {
            this.xmlRpcClient = new Client(url);
        } catch (MalformedURLException ex) {
            throw new FabricCommunicationException(ex);
        }
    }

    /**
     * Unwrap the underlying object from the Value wrapper.
     */
    private Object unwrapValue(Value v) {
        if (v.getType() == Value.TYPE_array) {
            return methodResponseArrayToList((Array) v.getValue());
        } else if (v.getType() == Value.TYPE_struct) {
            Map<String, Object> s = new HashMap<String, Object>();
            for (Member m : ((Struct) v.getValue()).getMember()) {
                s.put(m.getName(), unwrapValue(m.getValue()));
            }
            return s;
        }
        return v.getValue();
    }

    private List methodResponseArrayToList(Array array) {
        List result = new ArrayList();
        for (Value v : array.getData().getValue()) {
            result.add(unwrapValue(v));
        }
        return result;
    }

    public void setHeader(String name, String value) {
        this.xmlRpcClient.setHeader(name, value);
    }

    public void clearHeader(String name) {
        this.xmlRpcClient.clearHeader(name);
    }

    public List call(String methodName, Object args[]) throws FabricCommunicationException {
        MethodCall methodCall = new MethodCall();
        Params p = new Params();
        if (args == null) {
            args = new Object[] {};
        }
        for (int i = 0; i < args.length; ++i) {
            if (args[i] == null) {
                throw new NullPointerException("nil args unsupported");
            } else if (String.class.isAssignableFrom(args[i].getClass())) {
                p.addParam(new Param(new Value((String) args[i])));
            } else if (Double.class.isAssignableFrom(args[i].getClass())) {
                p.addParam(new Param(new Value((Double) args[i])));
            } else if (Integer.class.isAssignableFrom(args[i].getClass())) {
                p.addParam(new Param(new Value((Integer) args[i])));
            } else {
                throw new IllegalArgumentException("Unknown argument type: " + args[i].getClass());
            }
        }
        methodCall.setMethodName(methodName);
        methodCall.setParams(p);
        try {
            MethodResponse resp = this.xmlRpcClient.execute(methodCall);
            return methodResponseArrayToList((Array) resp.getParams().getParam().get(0).getValue().getValue());
        } catch (Exception ex) {
            throw new FabricCommunicationException("Error during call to `" + methodName + "' (args=" + args + ")", ex); //irrecoverable
        }
    }
}
