/***********************************************************************************************************************
 *
 * Mongo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2012 by Dawson Systems Ltd (http://www.dawsonsystems.com)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package com.dawsonsystems.session;

import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.CustomObjectInputStream;

import javax.servlet.http.HttpSession;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class JavaSerializer implements Serializer {
    private ClassLoader loader;

    @Override
    public void setClassLoader(ClassLoader loader) {
        this.loader = loader;
    }

    @Override
    public ByteBuffer serializeFrom(HttpSession session) throws IOException {

        StandardSession standardSession = (StandardSession) session;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gzos = new GZIPOutputStream(new BufferedOutputStream(bos));
        ObjectOutputStream oos = new ObjectOutputStream(gzos);
        oos.writeLong(standardSession.getCreationTime());
        standardSession.writeObjectData(oos);

        oos.close();
        gzos.finish();
        gzos.close();

        return ByteBuffer.wrap(bos.toByteArray());
    }

    @Override
    public HttpSession deserializeInto(ByteBuffer data, HttpSession session) throws IOException, ClassNotFoundException {

        StandardSession standardSession = (StandardSession) session;

        BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data.array()));

        GZIPInputStream gzis = new GZIPInputStream(bis);

        ObjectInputStream ois = new CustomObjectInputStream(gzis, loader);
        standardSession.setCreationTime(ois.readLong());
        standardSession.readObjectData(ois);

        return session;
    }
}
