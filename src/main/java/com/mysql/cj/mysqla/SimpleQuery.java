/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package com.mysql.cj.mysqla;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.mysql.cj.api.Query;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.CJTimeoutException;
import com.mysql.cj.core.exceptions.OperationCancelledException;
import com.mysql.cj.mysqla.io.CommandBuilder;

public class SimpleQuery implements Query {

    public MysqlaSession session = null;

    /** Used to identify this statement when profiling. */
    protected int statementId;

    /** Should we profile? */
    protected boolean profileSQL = false;

    protected ReadableProperty<Integer> maxAllowedPacket;

    protected CommandBuilder commandBuilder = new CommandBuilder(); // TODO use shared builder

    /** Mutex to prevent race between returning query results and noticing that we're timed-out or cancelled. */
    protected Object cancelTimeoutMutex = new Object();

    private CancelStatus cancelStatus = CancelStatus.NOT_CANCELED;

    /** Holds batched commands */
    protected List<Object> batchedArgs;

    public SimpleQuery(MysqlaSession sess) {
        this.session = sess;
        this.profileSQL = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValue();
        this.maxAllowedPacket = sess.getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
    }

    @Override
    public int getId() {
        return this.statementId;
    }

    @Override
    public void setId(int id) {
        this.statementId = id;
    }

    @Override
    public void setCancelStatus(CancelStatus cs) {
        this.cancelStatus = cs;
    }

    @Override
    public void checkCancelTimeout() {
        synchronized (this.cancelTimeoutMutex) {
            if (this.cancelStatus != CancelStatus.NOT_CANCELED) {
                CJException cause = this.cancelStatus == CancelStatus.CANCELED_BY_TIMEOUT ? new CJTimeoutException() : new OperationCancelledException();
                resetCancelledState();
                throw cause;
            }
        }
    }

    public void resetCancelledState() {
        synchronized (this.cancelTimeoutMutex) {
            this.cancelStatus = CancelStatus.NOT_CANCELED;
        }
    }

    @Override
    public <T extends Resultset> ProtocolEntityFactory<T> getResultSetFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MysqlaSession getSession() {
        return this.session;
    }

    @Override
    public Object getCancelTimeoutMutex() {
        return this.cancelTimeoutMutex;
    }

    public void closeQuery() {
        this.session = null;
    }

    public void addBatch(Object batch) {
        if (this.batchedArgs == null) {
            this.batchedArgs = new ArrayList<>();
        }
        this.batchedArgs.add(batch);
    }

    public List<Object> getBatchedArgs() {
        return this.batchedArgs == null ? null : Collections.unmodifiableList(this.batchedArgs);
    }

    @Override
    public void clearBatchedArgs() {
        if (this.batchedArgs != null) {
            this.batchedArgs.clear();
        }
    }

}
