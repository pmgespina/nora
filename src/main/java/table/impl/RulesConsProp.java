package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class RulesConsProp extends CassandraTable {

    public RulesConsProp(Database database) {
        super("RulesConsProp", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("ruleid", DataTypes.INT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("domain", DataTypes.TEXT);
        this.Columns.put("prop", DataTypes.TEXT);
        this.Columns.put("range", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final Integer ruleid;
        private final Integer num;
        private final String domain;
        private final String prop;
        private final String range;

        public Row(Integer ruleid, Integer num, String domain, String prop, String range) {
            this.ruleid = ruleid;
            this.num = num;
            this.domain = domain;
            this.prop = prop;
            this.range = range;
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

        @Override
        public String toString() {
            return MessageFormat.format("RulesConsProp'{'ruleid={0}, num={1}, domain={2}, prop={3}, range={4}'}'", ruleid, num, domain, prop, range);
        }
    }

}



