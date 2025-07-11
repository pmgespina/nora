package reasoner.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import database.Database;
import reasoner.ReasonerManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import table.impl.PropIndividuals;
import table.impl.RulesAntProp;
import table.impl.RulesConsProp;

public class ReasonerSWRLObjectProperty extends ReasonerManager{

    private final static Logger LOGGER = Logger.getLogger(ReasonerSWRLObjectProperty.class);

    public ReasonerSWRLObjectProperty(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSWRLObjectProperty(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple3<String, String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SWRLRules (object property)");

        for (Tuple3<String, String, String> tuple : inferences) {
            String prop = tuple._1();
            String domain = tuple._2();
            String range = tuple._3();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToPropIndividuals(prop, domain, range, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple3<String, String, String>> inference() {

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

        // (prop=:P, (ruleid=1, num=0, ..., domain=?x, range=?y), (domain=:Juan, range=:Maria))
        JavaPairRDD<String, Tuple2<RulesAntProp.Row, PropIndividuals.Row>> combinedRDD = 
                rulesAntPropRDD.join(propIndividualsRDD);

        // (range=:Maria, (ruleid=1, num=0, prop=:P, range=?y))
        JavaPairRDD<String, Tuple4<Integer, Integer, String, String>> RDD1 = 
                combinedRDD.mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(individualsRow.getRange(), new Tuple4<>(ruleRow.getRuleId(), ruleRow.getNum(), ruleRow.getProp(), ruleRow.getRange()));
                });

        // (domain=:Juan, (ruleid=1, num=0, prop=:P, domain=?x))
        JavaPairRDD<String, Tuple4<Integer, Integer, String, String>> RDD2 = 
                combinedRDD.mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(individualsRow.getDomain(), new Tuple4<>(ruleRow.getRuleId(), ruleRow.getNum(), ruleRow.getProp(), ruleRow.getDomain()));
                });

        // RDD de range: (ruleid, (rangeIndividual, ?y))
        JavaPairRDD<Integer, Tuple2<String, String>> rangeRDD = RDD1.mapToPair(row -> {
            Tuple4<Integer, Integer, String, String> data = row._2();  // (ruleid, num, prop, ?y)
            return new Tuple2<>(data._1(), new Tuple2<>(row._1(), data._4()));  // (:Maria, ?y)
        });

        // RDD de domain: (ruleid, (domainIndividual, ?x))
        JavaPairRDD<Integer, Tuple2<String, String>> domainRDD = RDD2.mapToPair(row -> {
            Tuple4<Integer, Integer, String, String> data = row._2();  // (ruleid, num, prop, ?x)
            return new Tuple2<>(data._1(), new Tuple2<>(row._1(), data._4()));  // (:Juan, ?x)
        });

        
        JavaPairRDD<Integer, Tuple2<Tuple2<String, String>, Tuple2<String, String>>> antecedentsRDD =
            rangeRDD.join(domainRDD);


        // Table RulesConsProp

        JavaPairRDD<Integer, RulesConsProp.Row> rulesConsPropRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesconsprop", CassandraJavaUtil.mapRowTo(RulesConsProp.Row.class))
                .keyBy((Function<RulesConsProp.Row, Integer>) RulesConsProp.Row::getRuleId);

        if (LOGGER.isDebugEnabled())
            rulesConsPropRDD.foreach(data -> {
                LOGGER.debug("RulesConsProp ruleid=" + data._1() + " row=" + data._2());
            });

        // Join antecedent with consequent rules

        // {(ruleid=1, (range=:Maria, range=?y), (domain=:Juan, domain=?x) (ruleid=1, num=0, domain=?x, prop=P, range=?y))}
        JavaPairRDD<Integer, Tuple3<Tuple2<String, String>, Tuple2<String, String>, RulesConsProp.Row>> joinedConsPropRDD = antecedentsRDD
            .join(rulesConsPropRDD)
            .mapToPair(row -> {
                Integer ruleId = row._1();
                Tuple2<Tuple2<String, String>, Tuple2<String, String>> antecedent = row._2()._1();  // ((:Maria, ?y), (:Juan, ?x))
                RulesConsProp.Row consequent = row._2()._2();

                return new Tuple2<>(ruleId, new Tuple3<>(antecedent._1(), antecedent._2(), consequent));
            });

        
        // Property inferences

        JavaPairRDD<String, Tuple2<String, String>> propertyInferencesRDD = joinedConsPropRDD
            .mapToPair(row -> {
                Tuple3<Tuple2<String, String>, Tuple2<String, String>, RulesConsProp.Row> tuple = row._2();

                Tuple2<String, String> rangeData = tuple._1();   // (:Maria, ?y)
                Tuple2<String, String> domainData = tuple._2();  // (:Juan, ?x)
                RulesConsProp.Row consequent = tuple._3();

                String prop = consequent.getProp();
                String domain = domainData._1();
                String range = rangeData._1();

                return new Tuple2<>(prop, new Tuple2<>(domain, range));
            });

        List<Tuple3<String, String, String>> inferences = propertyInferencesRDD
            .map(tuple -> new Tuple3<>(tuple._1(), tuple._2()._1(), tuple._2()._2()))
            .collect();

        // Result: (prop, domain, range)
        return inferences;
    }

}