package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class RulesConsClass extends CassandraTable {

    public RulesConsClass(Database database) {
        super("RulesConsClass", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("ruleid", DataTypes.INT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("cls", DataTypes.TEXT);
        this.Columns.put("var", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final Integer ruleid;
        private final Integer num;
        private final String cls;
        private final String var;

        public Row(Integer ruleid, Integer num, String cls, String var) {
            this.ruleid = ruleid;
            this.num = num;
            this.cls = cls;
            this.var = var;
        }

        public Integer getRuleId() {
            return ruleid;
        }

        public Integer getNum() {
            return num;
        }

        public String getCls() {
            return cls;
        }

        public String getVar() {
            return var;
        }

        @Override
        public String toString() {
            return MessageFormat.format("RulesConsClass'{'ruleid={0}, num={1}, cls={2}, var={3}'}'", ruleid, num, cls, var);
        }
    }

}



