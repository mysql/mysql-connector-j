/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package demo.x.devapi;

import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;

/*
 * Sample program showing how to use Connector/J's Dev API support.
 */
public class DevApiSample {
    public static void main(String[] args) {
        Session session = new SessionFactory().getSession("mysqlx://localhost:33060/test?user=user&password=password1234");
        System.err.println("Connected!");
        Schema schema = session.getDefaultSchema();
        System.err.println("Default schema is: " + schema);

        documentWalkthrough(schema);
    }

    public static void documentWalkthrough(Schema schema) {
        // document walthrough
        Collection coll = schema.createCollection("myBooks", /* reuseExisting? */ true);
        DbDoc newDoc = new DbDocImpl().add("isbn", new JsonString().setValue("12345"));
        newDoc.add("title", new JsonString().setValue("Effi Briest"));
        newDoc.add("author", new JsonString().setValue("Theodor Fontane"));
        newDoc.add("currentlyReadingPage", new JsonNumber().setValue(String.valueOf(42)));
        coll.add(newDoc).execute();

        // note: "$" prefix for document paths is optional. "$.title.somethingElse[0]" is the same as "title.somethingElse[0]" in document expressions
        DocResult docs = coll.find("$.title = 'Effi Briest' and $.currentlyReadingPage > 10").execute();
        DbDoc book = docs.next();
        System.err.println("Currently reading " + ((JsonString) book.get("title")).getString() + " on page "
                + ((JsonNumber) book.get("currentlyReadingPage")).getInteger());

        // increment the page number and fetch it again
        coll.modify("$.isbn = 12345").set("$.currentlyReadingPage", ((JsonNumber) book.get("currentlyReadingPage")).getInteger() + 1).execute();

        docs = coll.find("$.title = 'Effi Briest' and $.currentlyReadingPage > 10").execute();
        book = docs.next();
        System.err.println("Currently reading " + ((JsonString) book.get("title")).getString() + " on page "
                + ((JsonNumber) book.get("currentlyReadingPage")).getInteger());

        // remove the doc
        coll.remove("true").execute();
        System.err.println("Number of books in collection: " + coll.count());

        schema.dropCollection(coll.getName());
    }
}
