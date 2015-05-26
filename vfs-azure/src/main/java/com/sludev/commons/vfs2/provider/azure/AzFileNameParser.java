/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sludev.commons.vfs2.provider.azure;

import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for defining / parsing a provided FileName object.
 * 
 * This name should adhere to a URL structure, complete with an 'authority'
 * 
 * &lt;scheme&gt;://&lt;host_or_authority&gt;/&lt;container&gt;/&lt;file_path&gt;
 * E.g. azsb://myAccount.blob.core.windows.net/myContainer/path/to/file.txt
 * 
 */
public class AzFileNameParser extends URLFileNameParser
{
    private static final Logger log = LoggerFactory.getLogger(AzFileNameParser.class);
    
    private static final AzFileNameParser INSTANCE = new AzFileNameParser();

    public AzFileNameParser()
    {
        super(80);
    }

    public static FileNameParser getInstance()
    {
        return INSTANCE;
    }
}
