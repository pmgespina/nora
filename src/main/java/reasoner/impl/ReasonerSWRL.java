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
import scala.Tuple3;
import table.impl.IsComplementOf;

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
        
        // Tables
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

        JavaPairRDD<String, RulesConsProp.Row> rulesConsPropRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesconsprop", CassandraJavaUtil.mapRowTo(RulesConsProp.Row.class))
                .keyBy((Function<RulesConsProp.Row, String>) RulesConsProp.Row::getProp);

        if (LOGGER.isDebugEnabled())
            rulesConsPropRDD.foreach(data -> {
                LOGGER.debug("RulesConsProp prop=" + data._1() + " row=" + data._2());
            });
        
        JavaPairRDD<String, RulesConsClass.Row> rulesConsClassRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesconsclass", CassandraJavaUtil.mapRowTo(RulesConsClass.Row.class))
                .keyBy((Function<RulesConsClass.Row, String>) RulesConsClass.Row::getCls);        

        if (LOGGER.isDebugEnabled())
            rulesConsClassRDD.foreach(data -> {
                LOGGER.debug("RulesConsClass cls=" + data._1() + " row=" + data._2());
            });
        
        // Joins

        // (prop=:P, (ruleid=1, num=0, ..., domain=?x, range=?y), (:Juan, :Maria))
        JavaPairRDD<String, Tuple2<RulesAntProp.Row, PropIndividuals.Row>> combinedRDD = 
                rulesAntPropRDD.join(propIndividualsRDD);

        // (:Maria, (prop=:P, ?y))
        JavaPairRDD<String, Tuple2<String, String>> RDD1 = 
                combinedRDD.mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(individualsRow.getRange(), new Tuple2<>(ruleRow.getProp(), ruleRow.getRange()));
                });

        // (:Juan, (prop=:P, ?x))
        JavaPairRDD<String, Tuple2<String, String>> RDD2 = 
                combinedRDD.mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(individualsRow.getDomain(), new Tuple2<>(ruleRow.getProp(), ruleRow.getDomain()));
                });
        
        JavaPairRDD<String, Tuple2<String, String>> distinctRDD = RDD1.union(RDD2).distinct();

        // Hacemos join mediante el prop de la tabla de individuos y la tabla de reglas
        // Nos quedamos con la propiedad y los valores de los individuos en dominio y rango
        // (http://www.semanticweb.org/nora/pruebas2#P, (http://www.semanticweb.org/nora/pruebas2#a, http://www.semanticweb.org/nora/pruebas2#b))
        JavaPairRDD<String, Tuple2<String, String>> firstJoinRDD = 
                rulesAntPropRDD.join(propIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<RulesAntProp.Row, PropIndividuals.Row> tuple = row._2();

                    RulesAntProp.Row ruleRow = tuple._1();
                    PropIndividuals.Row individualsRow = tuple._2();

                    return new Tuple2<>(ruleRow.getProp(), new Tuple2<>(individualsRow.getDomain(), individualsRow.getRange()));
                });

        return null;
    }

}
