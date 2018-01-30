package utils;

import org.apache.commons.vfs2.FileObject;
import org.metaborg.core.MetaborgException;
import org.metaborg.core.language.ILanguageComponent;
import org.metaborg.core.language.ILanguageDiscoveryRequest;
import org.metaborg.core.language.ILanguageImpl;
import org.metaborg.core.language.LanguageUtils;
import org.metaborg.core.syntax.ParseException;
import org.metaborg.spoofax.core.Spoofax;
import org.metaborg.spoofax.core.unit.ISpoofaxInputUnit;
import org.metaborg.spoofax.core.unit.ISpoofaxParseUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spoofax.interpreter.terms.IStrategoTerm;

import java.io.IOException;
import java.net.URL;
import java.util.Set;

/**
 * Utility class to interface with the Spoofax parser. This class will always use the G-CORE
 * language component stored in the resources/ folder.
 */
public class Gcore {
    private static final String GCORE_GRAMMAR_SPEC =
            "gcore-spoofax-0.1.0-SNAPSHOT.spoofax-language";
    private final Logger loggger = LoggerFactory.getLogger(Gcore.class);
    private final ILanguageImpl gcore;
    private final Spoofax spoofax;

    public Gcore() throws MetaborgException {
        spoofax = new Spoofax();
        gcore = loadGcore(spoofax);
        if (gcore == null) {
            throw new MetaborgException("No language implementation was found");
        }
        loggger.debug("Loaded {}", gcore);
    }

    /** Parses a G-CORE query. */
    public IStrategoTerm parseQuery(String query) throws IOException, ParseException {
        ISpoofaxInputUnit input = spoofax.unitService.inputUnit(query, gcore, null);
        ISpoofaxParseUnit output = spoofax.syntaxService.parse(input);
        if(!output.valid()) {
            loggger.error("Could not parse query\n{}", query);
            return null;
        }
        IStrategoTerm ast = output.ast();
        return ast;
    }

    /** Loads the G-CORE language component. */
    private ILanguageImpl loadGcore(Spoofax spoofax) throws MetaborgException {
        URL gcoreUrl = Gcore.class.getClassLoader().getResource(GCORE_GRAMMAR_SPEC);
        FileObject gcoreLocation = spoofax.resourceService.resolve("zip:" + gcoreUrl + "!/");
        Iterable<ILanguageDiscoveryRequest> requests =
                spoofax.languageDiscoveryService.request(gcoreLocation);
        Iterable<ILanguageComponent> components =
                spoofax.languageDiscoveryService.discover(requests);
        Set<ILanguageImpl> implementations = LanguageUtils.toImpls(components);
        ILanguageImpl gcore = LanguageUtils.active(implementations);
        return gcore;
    }
}
