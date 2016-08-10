/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package demo.mysqlx;

// Dev API interfaces
import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.DocResult;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.XSession;
import com.mysql.cj.x.MysqlxSessionFactory;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonNumber;
import com.mysql.cj.x.json.JsonString;

/*
 * Sample program showing how to use Connector/J's Dev API support.
 */
public class DevApiSample {
    public static void main(String[] args) {
        XSession session = new MysqlxSessionFactory().getSession("mysqlx://localhost:33060/test?user=user&password=password1234");
        System.err.println("Connected!");
        Schema schema = session.getDefaultSchema();
        System.err.println("Default schema is: " + schema);

        documentWalkthrough(schema);
    }

    public static void documentWalkthrough(Schema schema) {
        // document walthrough
        Collection coll = schema.createCollection("myBooks", /* reuseExistingObject? */ true);
        DbDoc newDoc = new DbDoc().add("isbn", new JsonString().setValue("12345"));
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
        coll.remove().execute();
        System.err.println("Number of books in collection: " + coll.count());

        schema.getSession().dropCollection(schema.getName(), coll.getName());
    }
}
