package loader.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
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

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology, Imports imports) {
        return load(ontology, imports);
    }

    private Stream<SimpleStatement> load(OWLOntology ontology, Imports imports) {
        Set<SimpleStatement> collection = new HashSet<>();
        AtomicInteger ruleIdCounter = new AtomicInteger(1);

        ontology.axioms()
            .filter(axiom -> axiom instanceof SWRLRule)
            .map(axiom -> (SWRLRule) axiom)
            .forEach(rule -> {
                LOGGER.debug("Found SWRL rule " + rule);
                int ruleId = ruleIdCounter.getAndIncrement();

                Set<SWRLAtom> bodyAtoms = rule.body().collect(Collectors.toSet());
                Set<SWRLAtom> headAtoms = rule.head().collect(Collectors.toSet());

                extractAndInsertValuesAnt(bodyAtoms, ruleId, collection);
                extractAndInsertValuesCons(headAtoms, ruleId, collection);

            });

        LOGGER.info("Found " + collection.size() + " SWRL statements to insert");

        return collection.stream();    
    }

    private void extractAndInsertValuesAnt(Set<SWRLAtom> atoms, int ruleId, Set<SimpleStatement> collection) {
        for (SWRLAtom atom : atoms) {
            Map<String, Object> assignments = new HashMap<>();
            assignments.put("ruleid", ruleId);
            String prop = determineProp(atom);
            String domain = extractDomain(atom);
            String typedomain = determineTypeDomain(atom);
            String range = extractRange(atom);
            String typerange = determineTypeRange(atom);

            if (prop != null || domain != null || range != null || typedomain != null || typerange != null) {
                assignments.put("ruleid", ruleId);
                assignments.put("domain", domain); 
                assignments.put("prop", prop);
                assignments.put("range", range);
                assignments.put("typedomain", typedomain);
                assignments.put("typerange", typerange);

                // Usaremos statementIncrementalInsert para que la columna de num se me vaya sumando sola
                SimpleStatement query = rulesAntProp.statementIncrementalInsert(assignments);
                collection.add(query);
            }
        }
    }

    private void extractAndInsertValuesCons(Set<SWRLAtom> atoms, int ruleId, Set<SimpleStatement> collection) {
        for (SWRLAtom atom : atoms) {
            Map<String, Object> assignments = new HashMap<>();
            assignments.put("ruleid", ruleId);

            if (atom instanceof SWRLClassAtom) {
                String cls = determineCls(atom);
                String var = determineVar(atom);

                if (cls != null || var != null) {
                    assignments.put("cls", cls);
                    assignments.put("var", var);
                    SimpleStatement query = rulesConsClass.statementIncrementalInsert(assignments);
                    collection.add(query);
                }
            } else {
                String prop = determineProp(atom);
                String domain = extractDomain(atom);
                String typedomain = determineTypeDomain(atom);
                String range = extractRange(atom);
                String typerange = determineTypeRange(atom);

                if (prop != null || domain != null || range != null || typedomain != null || typerange != null) {
                    assignments.put("ruleid", ruleId);
                    assignments.put("domain", domain); 
                    assignments.put("prop", prop);
                    assignments.put("range", range);
                    assignments.put("typedomain", typedomain);
                    assignments.put("typerange", typerange);

                    // Usaremos statementIncrementalInsert para que la columna de num se me vaya sumando sola
                    SimpleStatement query = rulesConsProp.statementIncrementalInsert(assignments);
                    collection.add(query);
                }
            }
        }
    }

    private String determineTypeDomain(SWRLAtom atom) {
        return (atom instanceof SWRLObjectPropertyAtom) ? "var" : "ind";
    }

    private String determineTypeRange(SWRLAtom atom) {
        return (atom instanceof SWRLObjectPropertyAtom) ? "var" : "ind";
    }

    private String extractDomain(SWRLAtom atom) {
        if (atom instanceof SWRLObjectPropertyAtom) {
            SWRLObjectPropertyAtom propAtom = (SWRLObjectPropertyAtom) atom;
            SWRLArgument argDom = propAtom.getFirstArgument();
            return extractIRI(argDom);
        } else {
            return null;
        }
    }

    private String extractRange(SWRLAtom atom) {
        if (atom instanceof SWRLObjectPropertyAtom) {
            SWRLObjectPropertyAtom propAtom = (SWRLObjectPropertyAtom) atom;
            SWRLArgument argRang = propAtom.getSecondArgument();
            return extractIRI(argRang);
        } else {
            return null;
        }
    }

    private String determineProp(SWRLAtom atom) {
        if (atom instanceof SWRLObjectPropertyAtom) {
            SWRLObjectPropertyAtom propAtom = (SWRLObjectPropertyAtom) atom;
            return propAtom.getPredicate().asOWLObjectProperty().getIRI().getIRIString();
        } else {
            return null;
        }
    }

    private String determineVar(SWRLAtom atom) {
        if (atom instanceof SWRLClassAtom)  {
            SWRLClassAtom classAtom = (SWRLClassAtom) atom;
            SWRLArgument classArgument = classAtom.getArgument();
            return extractIRI(classArgument);
        } else {
            return null;
        }
    }

    private String determineCls(SWRLAtom atom) {
        if (atom instanceof SWRLClassAtom) {
            return ((SWRLClassAtom) atom).getPredicate().asOWLClass().getIRI().getIRIString();
        } else {
            return null;
        }
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

    // Depending on the type of SWRLArgument, we extract the IRI in a different way
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
