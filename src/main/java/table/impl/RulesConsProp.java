package table.impl;

import java.io.Serializable;
import java.text.MessageFormat;

import com.datastax.oss.driver.api.core.type.DataTypes;

import database.Database;
import table.CassandraTable;

public class RulesConsProp extends CassandraTable {

    public RulesConsProp(Database database) {
        super("RulesConsProp", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("ruleid", DataTypes.INT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("domain", DataTypes.TEXT);
        this.Columns.put("prop", DataTypes.TEXT);
        this.Columns.put("range", DataTypes.TEXT);
        this.Columns.put("typedomain", DataTypes.TEXT);
        this.Columns.put("typerange", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final Integer ruleid;
        private final Integer num;
        private final String domain;
        private final String prop;
        private final String range;
        private final String typedomain;
        private final String typerange;

        public Row(Integer ruleid, Integer num, String domain, String prop, String range, String typedomain, String typerange) {
            this.ruleid = ruleid;
            this.num = num;
            this.domain = domain;
            this.prop = prop;
            this.range = range;
            this.typedomain = typedomain;
            this.typerange = typerange; 
        }

        public Integer getRuleId() {
            return ruleid;
        }

        public Integer getNum() {
            return num;
        }

        public String getDomain() {
            return domain;
        }

        public String getProp() {
            return prop;
        }

        public String getRange() {
            return range;
        }

        public String getTypeDomain() {
            return typedomain;
        }

        public String getTypeRange() {
            return typerange;
        }

        @Override
        public String toString() {
            return MessageFormat.format("RulesConsProp'{'ruleid={0}, num={1}, domain={2}, prop={3}, range={4}, typedomain={5}, typerange={6}'}'", 
                    ruleid, num, domain, prop, range, typedomain, typerange);
        }
    }

}



