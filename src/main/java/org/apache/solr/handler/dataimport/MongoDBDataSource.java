package org.apache.solr.handler.dataimport;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;


public class MongoDBDataSource extends
        DataSource<Iterator<Map<String, Object>>> {
    private static final Logger logger = LoggerFactory
            .getLogger(MongoDBDataSource.class);

    protected DB mongoDB;
    private DBCollection collection;
    private DBCursor cursor;

    @Override
    public void init(Context context, Properties initProps) {
        String host = initProps.getProperty(MONGO_HOST, DEFAULT_MONGO_HOST);
        int port = Integer.parseInt(initProps.getProperty(MONGO_PORT,
                DEFAULT_MONGO_PORT));
        String username = initProps.getProperty(MONGO_USERNAME);
        String password = initProps.getProperty(MONGO_PASSWORD);
        String dbName = initProps.getProperty(MONGO_DATABASE);

        if (dbName == null) {
            throw new DataImportHandlerException(SEVERE,
                    "database can not be null");
        }

        try {
            Mongo mongo = new MongoClient(host, port);
            this.mongoDB = mongo.getDB(dbName);

            if (username != null) {
                boolean auth = this.mongoDB.authenticate(username,
                        password.toCharArray());
                if (!auth) {
                    throw new DataImportHandlerException(SEVERE,
                            "auth failed with username: " + username
                                    + ", password: " + password);
                }
            }
            logger.info(String.format("mongodb [%s:%d]@%s inited", host, port, dbName));
        } catch (Exception e) {
            throw new DataImportHandlerException(SEVERE, "init mongodb failed");
        }
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {
        DBObject queryObject = (DBObject) JSON.parse(query);
        cursor = collection.find(queryObject);
        ResultSetIterator resultSet = new ResultSetIterator(cursor);
        return resultSet.getIterator();
    }

    public Iterator<Map<String, Object>> getData(String query,
                                                 String collectionName) {
        logger.info(String.format("query mongodb with cmd: %s at collection: %s", query, collectionName));
        this.collection = mongoDB.getCollection(collectionName);
        return getData(query);
    }

    private class ResultSetIterator {
        DBCursor mCursor;

        Iterator<Map<String, Object>> resultSet;

        public ResultSetIterator(DBCursor MongoCursor) {
            this.mCursor = MongoCursor;

            resultSet = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return ResultSetIterator.this.hasNext();
                }

                public Map<String, Object> next() {
                    return getNext();
                }

                public void remove() {
                }
            };

        }

        public Iterator<Map<String, Object>> getIterator() {
            return resultSet;
        }

        private Map<String, Object> getNext() {
            DBObject mongoObject = getMongoCursor().next();

            Set<String> keys = mongoObject.keySet();
            Map<String, Object> result = new HashMap<String, Object>(keys.size());

            for (String key : keys) {
                Object value = mongoObject.get(key);
                result.put(key, value);
            }

            return result;
        }

        private boolean hasNext() {
            if (mCursor == null)
                return false;
            try {
                if (mCursor.hasNext()) {
                    return true;
                } else {
                    close();
                    return false;
                }
            } catch (MongoException e) {
                close();
                wrapAndThrow(SEVERE, e);
                return false;
            }
        }

        private void close() {
            try {
                if (mCursor != null)
                    mCursor.close();
            } catch (Exception e) {
                logger.warn("Exception while closing result set", e);
            } finally {
                mCursor = null;
            }
        }
    }

    private DBCursor getMongoCursor() {
        return this.cursor;
    }

    @Override
    public void close() {
        if (this.cursor != null) {
            this.cursor.close();
        }

    }

    public static final String MONGO_HOST = "host";
    public static final String MONGO_PORT = "port";
    public static final String MONGO_USERNAME = "username";
    public static final String MONGO_PASSWORD = "password";
    public static final String MONGO_DATABASE = "database";

    public static final String DEFAULT_MONGO_HOST = "localhost";
    public static final String DEFAULT_MONGO_PORT = "27017";

}
