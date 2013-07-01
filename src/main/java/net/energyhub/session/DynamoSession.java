/***********************************************************************************************************************
 *
 * Dynamo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2012 by Dawson Systems Ltd (http://www.dawsonsystems.com)
 * Copyright (C) 2013 by EnergyHub Inc. (http://www.energyhub.com)
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

package net.energyhub.session;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

public class DynamoSession extends StandardSession {
    private boolean isValid = true;

    public DynamoSession(Manager manager) {
        super(manager);
    }

    @Override
    protected boolean isValidInternal() {
        return isValid;
    }

    @Override
    public boolean isValid() {
        return isValidInternal();
    }

    @Override
    public void setValid(boolean isValid) {
        this.isValid = isValid;
        if (!isValid) {
            String keys[] = keys();
            for (String key : keys) {
                removeAttributeInternal(key, false);
            }
            getManager().remove(this);

        }
    }

    @Override
    public void invalidate() {
        setValid(false);
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public AttributeValue getAttributeValue() {
        return new AttributeValue().withS(this.id);
    }

    public void setLastAccessedTime(long accessTime) {
        this.thisAccessedTime = accessTime;
        this.lastAccessedTime = accessTime;
    }
}
