package loader.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.SWRLArgument;
import org.semanticweb.owlapi.model.SWRLAtom;
import org.semanticweb.owlapi.model.SWRLClassAtom;
import org.semanticweb.owlapi.model.SWRLIndividualArgument;
import org.semanticweb.owlapi.model.SWRLObjectPropertyAtom;
import org.semanticweb.owlapi.model.SWRLRule;
import org.semanticweb.owlapi.model.SWRLVariable;
import org.semanticweb.owlapi.model.parameters.Imports;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import database.Database;
import loader.LoaderManager;
import table.impl.RulesAntProp;
import table.impl.RulesConsClass;
import table.impl.RulesConsProp;

public class LoaderSWRL extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderSWRL.class);

    private RulesAntProp rulesAntProp;
    private RulesConsClass rulesConsClass;
    private RulesConsProp rulesConsProp;

    public LoaderSWRL(Database db, OWLOntologyManager manager) {
        super(db, manager);
    }

    @Override
    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        return load(ontology, Imports.INCLUDED);
    }

    private Stream<SimpleStatement> load(OWLOntology ontology, Imports imports) {
        Set<SimpleStatement> collection = new HashSet<>();

        Stream<SWRLRule> rules = ontology.axioms(AxiomType.SWRL_RULE, imports);

        AtomicInteger ruleIdCounter = new AtomicInteger(1);

        rules.forEach(rule -> {

            int ruleId = ruleIdCounter.getAndIncrement();
            LOGGER.debug("Found SWRL rule " + rule);
        
            Set<SWRLAtom> body = rule.getBody(); // Antecedente. Solo propiedades de objeto
            Set<SWRLAtom> head = rule.getHead(); // Consecuente. Propiedades de objeto y de clase

            body.forEach(atom -> {
                // Como el tipo de atomo solo puede ser clausula de propiedad de objeto, asumimos que es asi
                SWRLObjectPropertyAtom propAtom = (SWRLObjectPropertyAtom) atom;
                String prop = propAtom.getPredicate().asOWLObjectProperty().getIRI().getIRIString();
                SWRLArgument argDom = propAtom.getFirstArgument();
                String dom = extractIRI(argDom);
                SWRLArgument argRang = propAtom.getSecondArgument();
                String rang = extractIRI(argRang);

                HashMap<String, Object> assignments = new HashMap<>();
                assignments.put("ruleid", ruleId);
                assignments.put("domain", dom); 
                assignments.put("prop", prop);
                assignments.put("range", rang);

                // Usaremos statementIncrementalInsert para que la columna de num se me vaya sumando sola
                SimpleStatement query = rulesAntProp.statementIncrementalInsert(assignments);
                collection.add(query);
            });

            head.forEach(atom -> {
                HashMap<String, Object> assignments = new HashMap<>();
                if (atom instanceof SWRLClassAtom) {
                    SWRLClassAtom classAtom = (SWRLClassAtom) atom;
                    String cls = classAtom.getPredicate().asOWLClass().getIRI().getIRIString();
                    SWRLArgument argVar = classAtom.getArgument();
                    String var = extractIRI(argVar);

                    assignments.put("ruleid", ruleId);
                    assignments.put("cls", cls);
                    assignments.put("var", var);

                    SimpleStatement query = rulesConsClass.statementIncrementalInsert(assignments);
                    collection.add(query);
                } else if (atom instanceof SWRLObjectPropertyAtom) {
                    SWRLObjectPropertyAtom propAtom = (SWRLObjectPropertyAtom) atom;
                    String prop = propAtom.getPredicate().asOWLObjectProperty().getIRI().getIRIString();
                    SWRLArgument argDom = propAtom.getFirstArgument();
                    String dom = extractIRI(argDom);
                    SWRLArgument argRang = propAtom.getSecondArgument();
                    String rang = extractIRI(argRang);

                    assignments.put("ruleid", ruleId);
                    assignments.put("domain", dom); 
                    assignments.put("prop", prop);
                    assignments.put("range", rang);

                    SimpleStatement query = rulesConsProp.statementIncrementalInsert(assignments);
                    collection.add(query);
                }
            });
        });

        LOGGER.info("Found " + collection.size() + " SWRL statements to insert");

        return collection.stream();    
    }

    @Override
    public void initializeTables() {
        rulesAntProp = new RulesAntProp(connection);
        rulesAntProp.initialize();

        rulesConsClass = new RulesConsClass(connection);
        rulesConsClass.initialize();

        rulesConsProp = new RulesConsProp(connection);
        rulesConsProp.initialize();
    }

    private String extractIRI(SWRLArgument arg) {
        if (arg instanceof SWRLVariable) {
            return ((SWRLVariable) arg).getIRI().getIRIString();
        } else if (arg instanceof SWRLIndividualArgument) {
            return ((SWRLIndividualArgument) arg)
                    .getIndividual()
                    .asOWLNamedIndividual()
                    .getIRI()
                    .getIRIString();
        } else {
            throw new IllegalArgumentException("Unsupported SWRL argument type: " + arg.getClass());
        }
    }
                                          

}
