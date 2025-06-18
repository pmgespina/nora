package reasoner.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import database.Database;
import reasoner.ReasonerManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;
import scala.Tuple4;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import table.impl.PropIndividuals;
import table.impl.RulesAntProp;
import table.impl.RulesConsClass;
import table.impl.RulesConsProp;

public class ReasonerSWRL extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSWRL.class);

    public ReasonerSWRL(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSWRL(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    @Override
    public Integer resolve(List<Tuple2<String, String>> inferences) {
        // Implement SWRL resolution logic here
        return 0; // Placeholder return value
    }

    @Override
    public List<Tuple2<String, String>> inference() {
        
        // Tables RulesAntProp and PropIndividuals

        JavaPairRDD<String, RulesAntProp.Row> rulesAntPropRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesantprop", CassandraJavaUtil.mapRowTo(RulesAntProp.Row.class))
                .keyBy((Function<RulesAntProp.Row, String>) RulesAntProp.Row::getProp);

        if (LOGGER.isDebugEnabled())
            rulesAntPropRDD.foreach(data -> {
                LOGGER.debug("RulesAntProp prop=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("PropIndividuals prop=" + data._1() + " row=" + data._2());
            });
        
        // Joins for antecedent part of SWRL rules

        // (prop=:P, (ruleid=1, num=0, ..., domain=?x, range=?y), (:Juan, :Maria))
        JavaPairRDD<String, Tuple2<RulesAntProp.Row, PropIndividuals.Row>> combinedRDD = 
                rulesAntPropRDD.join(propIndividualsRDD);

        // (:Maria, (ruleid=1, num=0, prop=:P, range=?y))
        JavaPairRDD<String, Tuple4<Integer, Integer, String, String>> RDD1 = 
                combinedRDD.mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(individualsRow.getRange(), new Tuple4<>(ruleRow.getRuleId(), ruleRow.getNum(), ruleRow.getProp(), ruleRow.getRange()));
                });

        // (:Juan, (ruleid=1, num=0, prop=:P, domain=?x))
        JavaPairRDD<String, Tuple4<Integer, Integer, String, String>> RDD2 = 
                combinedRDD.mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(individualsRow.getDomain(), new Tuple4<>(ruleRow.getRuleId(), ruleRow.getNum(), ruleRow.getProp(), ruleRow.getDomain()));
                });
        
        // (:Maria, (ruleid=1, num=0, prop=:P, range=?y), :Juan, (ruleid=1, num=0, prop=:P, domain=?x))
        JavaPairRDD<String, Tuple4<Integer, Integer, String, String>> distinctRDD = RDD1.union(RDD2).distinct();

        // Tables RulesConsProp and RulesConsClass

        JavaPairRDD<Integer, RulesConsProp.Row> rulesConsPropRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesconsprop", CassandraJavaUtil.mapRowTo(RulesConsProp.Row.class))
                .keyBy((Function<RulesConsProp.Row, Integer>) RulesConsProp.Row::getRuleId);

        if (LOGGER.isDebugEnabled())
            rulesConsPropRDD.foreach(data -> {
                LOGGER.debug("RulesConsProp ruleid=" + data._1() + " row=" + data._2());
            });
        
        JavaPairRDD<Integer, RulesConsClass.Row> rulesConsClassRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesconsclass", CassandraJavaUtil.mapRowTo(RulesConsClass.Row.class))
                .keyBy((Function<RulesConsClass.Row, Integer>) RulesConsClass.Row::getRuleId);        

        if (LOGGER.isDebugEnabled())
            rulesConsClassRDD.foreach(data -> {
                LOGGER.debug("RulesConsClass ruleid=" + data._1() + " row=" + data._2());
            });


        // Join RulesConsProp and RulesConsClass for properties and class inferences

        JavaPairRDD<Integer, Tuple2<RulesConsProp.Row, RulesConsClass.Row>> rulesConsRDD = 
                rulesConsPropRDD.join(rulesConsClassRDD);


        return null;
    }

}
