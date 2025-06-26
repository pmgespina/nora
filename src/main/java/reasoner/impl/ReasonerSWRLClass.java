package reasoner.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import table.impl.PropIndividuals;
import table.impl.RulesAntProp;
import table.impl.RulesConsClass;

public class ReasonerSWRLClass extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSWRLClass.class);

    public ReasonerSWRLClass(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSWRLClass(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SWRLRules (class)");

        for (Tuple2<String, String> tuple : inferences) {
            String cls = tuple._1();
            String ind = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(cls, ind, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        
        // Tables RulesAntProp, PropIndividuals y RulesConsClass

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
            
        // Join I

        JavaPairRDD<Integer, Tuple2<RulesAntProp.Row, PropIndividuals.Row>> ruleIdKeyedRDD =
            rulesAntPropRDD.join(propIndividualsRDD)
                    .mapToPair(tuple -> {
                        RulesAntProp.Row rule = tuple._2()._1();
                        PropIndividuals.Row ind = tuple._2()._2();
                        return new Tuple2<>(rule.getRuleId(), new Tuple2<>(rule, ind));
                    });

        JavaPairRDD<Integer, Iterable<Tuple2<RulesAntProp.Row, PropIndividuals.Row>>> atomsPerRule = ruleIdKeyedRDD.groupByKey();

        JavaPairRDD<Integer, Tuple2<String, String>> groundedMatches = atomsPerRule.flatMapToPair(ruleTuple -> {

            Integer ruleId = ruleTuple._1();
            Iterable<Tuple2<RulesAntProp.Row, PropIndividuals.Row>> atomsIterable = ruleTuple._2();

            List<Tuple2<RulesAntProp.Row, PropIndividuals.Row>> atoms =
                StreamSupport.stream(atomsIterable.spliterator(), false)
                            .collect(Collectors.toList());

            // The first atom of the SWRL rule
            Tuple2<RulesAntProp.Row, PropIndividuals.Row> firstAtom = atoms.get(0);
            RulesAntProp.Row rule1 = firstAtom._1();
            PropIndividuals.Row ind1 = firstAtom._2();

            List<Map<String, String>> prevBindings = new ArrayList<>();
            Map<String, String> initialBinding = new HashMap<>();
            initialBinding.put(rule1.getDomain(), ind1.getDomain());
            initialBinding.put(rule1.getRange(), ind1.getRange());
            prevBindings.add(initialBinding);

            // Next atoms
            for (int i = 1; i < atoms.size(); i++) {
                RulesAntProp.Row rule = atoms.get(i)._1();
                PropIndividuals.Row ind = atoms.get(i)._2();

                String domainVariable = rule.getDomain();
                String rangeVariable = rule.getRange();
                String domainIndividual = ind.getDomain();
                String rangeIndividual = ind.getRange();

                List<Map<String, String>> nextBindings = new ArrayList<>();

                for (Map<String, String> binding : prevBindings) {
                    // We verify if the relationship between the domain and range variables is OK
                    if (binding.containsKey(domainVariable) &&
                        binding.get(domainVariable).equals(domainIndividual)) {

                        Map<String, String> extendedBinding = new HashMap<>(binding);
                        extendedBinding.put(domainVariable, domainIndividual);
                        extendedBinding.put(rangeVariable, rangeIndividual);
                        nextBindings.add(extendedBinding);
                    }
                }

                prevBindings = nextBindings;
            }

            List<Tuple2<Integer, Tuple2<String, String>>> resultsWithRuleId = prevBindings.stream()
                .flatMap(binding -> binding.entrySet().stream()
                    .map(entry -> new Tuple2<>(ruleId, new Tuple2<>(entry.getKey(), entry.getValue()))))
                .collect(Collectors.toList());

            return resultsWithRuleId.iterator();
        });

        JavaPairRDD<Integer, RulesConsClass.Row> rulesConsClassRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "rulesconsclass", CassandraJavaUtil.mapRowTo(RulesConsClass.Row.class))
                .keyBy((Function<RulesConsClass.Row, Integer>) RulesConsClass.Row::getRuleId);

        if (LOGGER.isDebugEnabled())
            rulesAntPropRDD.foreach(data -> {
                LOGGER.debug("RulesConsClass ruleid=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<Integer, Tuple2<Tuple2<String, String>, RulesConsClass.Row>> joined = groundedMatches.join(rulesConsClassRDD);

        JavaPairRDD<Integer, Tuple2<Tuple2<String, String>, RulesConsClass.Row>> filtered = joined.filter(tuple -> {
            Tuple2<String, String> variablesIndividuals = tuple._2()._1();
            RulesConsClass.Row cons = tuple._2()._2();

            String variable = variablesIndividuals._1();
            String varsConsecuente = cons.getVar(); 

            return variable.equals(varsConsecuente);
        });

        JavaPairRDD<String, String> classIndividualPairs = filtered.mapToPair(tuple -> {
            Tuple2<String, String> variablesIndividuals = tuple._2()._1();
            RulesConsClass.Row cons = tuple._2()._2();

            String individual = variablesIndividuals._2();
            String class = cons.getCls();

            Tuple2<String, String> classIndividual = new Tuple2<>(class, individual);

            return classIndividual;
        });


        // Result: (class, individual)
        return classIndividualPairs.collect().stream()
                .map(tuple -> new Tuple2<>(tuple._1(), tuple._2()))
                .collect(Collectors.toList());
    }

}
