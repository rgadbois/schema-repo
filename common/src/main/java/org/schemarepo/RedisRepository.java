/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.schemarepo;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Collections;

import javax.inject.Inject;
import javax.inject.Named;

import org.schemarepo.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

/**
 * A {@link Repository} that persists content to redis cluster. <br/>
 * <br/>
 * The {@link Repository} stores all of its data using key/value paris in redis.
 * Each subject key will store an array of schemas.
 *
 */
public class RedisRepository extends AbstractBackendRepository {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String redisUrl;
    private final ShardedJedis jedis;
    /**
     * Create a RedisRepository using the redis url provided. Single shard support at this time.
     *
     * @param redisUrl The path where to store the Repository's state
     */
    @Inject
    public RedisRepository(@Named(Config.REDIS_URL) String redisUrl, ValidatorFactory validators) {
        super(validators);
        this.redisUrl = redisUrl;
        String[] serversHostPort = redisUrl.split(",");

        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        for (String serverHostPort : serversHostPort) {
            String[] hostPort = serverHostPort.split(":");
            shards.add(new JedisShardInfo(hostPort[0], Integer.parseInt(hostPort[1])));
            if (logger.isTraceEnabled()) {
                logger.trace("Connecting to " + hostPort[0] + ":" + hostPort[1]);
            }
        }
        jedis = new ShardedJedis(shards);

        // eagerly load up subjects
        SubjectConfig config = null;
        Set<String> subjects = jedis.smembers(Config.SUBJECTS_KEY);
        for (String subjectName : subjects) {
            Map<String, String> propData = jedis.hgetAll(Config.SUBJECT_KEY_PATH + subjectName + Config.CONFIG_KEY_PATH);
            config = new SubjectConfig.Builder().set(propData).build();
            subjectCache.add(new RedisSubject(subjectName, config, jedis));
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            jedis.disconnect();
        } catch (Exception e) {
            logger.debug("Failed to disconnect from redis", e);
        } finally {
            closed = true;

        }
        try {
            super.close();
        } catch (IOException e) {
            // should never happen
        }
    }

    @Override
    protected Subject getSubjectInstance(final String subjectName) {
        final Subject subject = subjectCache.lookup(subjectName);
        if (subject == null) {
            throw new IllegalStateException("Unexpected: subject must've been cached by #registerSubjectInBackend");
        }
        return subject;
    }
    @Override
    protected void registerSubjectInBackend(final String subjectName, final SubjectConfig config) {
        jedis.sadd(Config.SUBJECTS_KEY, subjectName);
        final Map<String, String> values = config.asMap();
        for (String key : values.keySet()) {
            jedis.hset(Config.SUBJECT_KEY_PATH + subjectName + Config.CONFIG_KEY_PATH, key, values.get(key));
        }
        cacheSubject(new RedisSubject(subjectName, config, jedis));
    }


    @Override
    protected Map<String, String> exposeConfiguration() {
        final Map<String, String> properties = new LinkedHashMap<String, String>(super.exposeConfiguration());
        properties.put(Config.REDIS_URL, redisUrl);
        return properties;
    }


    private static class RedisSubject extends Subject {
        //private final InMemorySchemaEntryCache schemas = new InMemorySchemaEntryCache();
        //private SchemaEntry latest = null;
        private final SubjectConfig config;
        private final ShardedJedis jedis;

        protected RedisSubject(String name, SubjectConfig config, ShardedJedis jedis) {
            super(name);
            this.config = RepositoryUtil.safeConfig(config);
            this.jedis = jedis;
        }

        @Override
        public SubjectConfig getConfig() {
            return config;
        }

        @Override
        public synchronized SchemaEntry register(String schema)
                throws SchemaValidationException {

            final Long last = jedis.incr(Config.SUBJECT_KEY_PATH + getName() + Config.LAST_KEY_PATH);
            jedis.hset(Config.SUBJECT_KEY_PATH + getName(), last.toString(), schema);

            return new SchemaEntry(last.toString(), schema);
        }

        @Override
        public synchronized SchemaEntry registerIfLatest(String schema,
                                                         SchemaEntry latest) throws SchemaValidationException {
            // this function is redonkulous.
            final SchemaEntry redisLatest = latest();
            if ((latest != null) && latest.equals(redisLatest)) {
                return register(schema);
            } else {
                return null;
            }
        }

        @Override
        public SchemaEntry lookupBySchema(String schema) {
            final Map<String,String> redisSchemas = jedis.hgetAll(Config.SUBJECT_KEY_PATH + getName());
            if ((redisSchemas != null) && !redisSchemas.isEmpty()) {
                for (String key : redisSchemas.keySet()) {
                    String value = redisSchemas.get(key);
                    if (value.equals(schema)) {
                        return new SchemaEntry(key, value);
                    }
                }
            }
            return null;
        }

        @Override
        public SchemaEntry lookupById(String id) {
            final String schema = jedis.hget(Config.SUBJECT_KEY_PATH + getName(), id);
            if ((schema != null) && !schema.isEmpty()) {
                return new SchemaEntry(id, schema);
            }
            return null;
        }

        @Override
        public synchronized SchemaEntry latest() {
            final String last = jedis.get(Config.SUBJECT_KEY_PATH + getName() + Config.LAST_KEY_PATH);
            if ((last != null) && !last.isEmpty()) {
                return  lookupById(last);
            }
            return null;
        }

        @Override
        public synchronized Iterable<SchemaEntry> allEntries() {
            final List<SchemaEntry> entries = new ArrayList<SchemaEntry>();
            final Map<String,String> redisSchemas = jedis.hgetAll(Config.SUBJECT_KEY_PATH + getName());
            if ((redisSchemas != null) && !redisSchemas.isEmpty()) {
                for (String key : redisSchemas.keySet()) {
                    entries.add(new SchemaEntry(key, redisSchemas.get(key)));
                }
                Collections.reverse(entries);
            }
            return entries;
        }

        @Override
        public boolean integralKeys() {
            return true;
        }
    }

}
