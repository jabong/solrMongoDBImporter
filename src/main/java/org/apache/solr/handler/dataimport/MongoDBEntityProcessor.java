package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;


public class MongoDBEntityProcessor extends EntityProcessorBase {
    private static final Logger LOG = LoggerFactory
            .getLogger(MongoDBEntityProcessor.class);
    protected MongoDBDataSource mongoDBDataSource;
    private String collection;

    @Override
    public void init(Context context) {
        super.init(context);
        collection = context.getEntityAttribute(COLLECTION);
        if (collection == null) {
            throw new DataImportHandlerException(SEVERE, "collection is null");
        }
        mongoDBDataSource = (MongoDBDataSource) context.getDataSource();
    }

    protected void initQuery(String q) {
        try {
            q = replaceDateTimeToISODateTime(q);
            System.out.println("--------------------------:" + q);
            DataImporter.QUERY_COUNT.get().incrementAndGet();
            rowIterator = mongoDBDataSource.getData(q, collection);
            this.query = q;
        } catch (DataImportHandlerException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("query failed: [" + query + "]", e);
            throw new DataImportHandlerException(SEVERE, e);
        }
    }

    @Override
    public Map<String, Object> nextRow() {
        if (rowIterator == null) {
            String q = getQuery();
            initQuery(q);
        }
        Map<String, Object> data = getNext();
        LOG.debug("process: " + data);
        return data;
    }

    @Override
    public Map<String, Object> nextModifiedRowKey() {
        if (rowIterator == null) {
            String deltaQuery = context.getEntityAttribute(DELTA_QUERY);
            if (deltaQuery == null)
                return null;
            deltaQuery = getQuery();
            initQuery(context.replaceTokens(deltaQuery));
        }
        return getNext();
    }

//        SimpleDateFormat sdf = DataImporter.DATE_TIME_FORMAT.get();
    private static final Pattern Date_PATTERN = Pattern.compile("(\\d{4}[-|\\\\/]\\d{1,2}[-|\\\\/]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2})");
    private String replaceDateTimeToISODateTime(String s){
        Matcher m = Date_PATTERN.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String srcStr = m.group(1);
            String dstStr = srcStr.substring(0, 10) + "T" + srcStr.substring(11) +"Z";
            m.appendReplacement(sb, dstStr);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public String getQuery() {
        String queryString = context.getEntityAttribute(QUERY);
        if (Context.FULL_DUMP.equals(context.currentProcess())) {
            return queryString;
        }
        if (Context.DELTA_DUMP.equals(context.currentProcess()) || Context.FIND_DELTA.equals(context.currentProcess())) {
            String deltaImportQuery = context.getEntityAttribute(DELTA_IMPORT_QUERY);
            if (deltaImportQuery != null) return getDeltaImportQuery(deltaImportQuery);
        }
        LOG.warn("'deltaImportQuery' attribute is not specified for entity : " + entityName);
        return getDeltaImportQuery(queryString);
    }

    public String getDeltaImportQuery(String queryString) {
        return queryString;
    }

    public static final String COLLECTION = "collection";

    public static final String QUERY = "query";

    public static final String DELTA_QUERY = "deltaQuery";

    public static final String DELTA_IMPORT_QUERY = "deltaImportQuery";
}
