package pl.net.lkd.nonwords

import com.gs.collections.impl.map.mutable.UnifiedMap
import com.gs.collections.impl.map.mutable.primitive.ByteByteHashMap
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.postgresql.ds.PGSimpleDataSource

@Slf4j
class App {

    ConfigObject CONFIG
    Set WORDLIST
    Map MODEL

    def dumpWordlist() {
        log.info("Generating word list at ${CONFIG.app.wordlist}.")

        PGSimpleDataSource ds = new PGSimpleDataSource()
        ds.serverName = CONFIG.db.host
        ds.user = CONFIG.db.user
        ds.password = CONFIG.db.password
        ds.databaseName = CONFIG.db.database
        new File(CONFIG.app.wordlist).withWriter { out ->
            def idx = 0
            ['spoken', 'written'].each { schema ->

                Sql db = new Sql(ds)
                db.eachRow("SELECT hw FROM ${schema}.word WHERE type='WORD'" as String) { row ->
                    idx++
                    out.writeLine(row.hw.toLowerCase())
                    if (idx % 1000000 == 0) {
                        log.info("\t${idx} rows retrieved...")
                        out.flush()
                    }
                }
                db.close()
            }
        }

        log.info("Word list written.")
    }

    def buildMM0() {
        log.info("Building Markov chain at ${CONFIG.app.model}.")

        UnifiedMap<String, String> ngrams = new UnifiedMap<>(100000000)

        def ngramize = { List word ->
            def n = [word.size(), CONFIG.app.window].min()
            (-n..-1).each {
                def k = (['START'] * (-it) + word[0..<n + it]).join('|')
                if (!ngrams.containsKey(k)) {
                    ngrams[k] = []
                }
                ngrams[k] << word[n + it]
            }
            (n - 1..<word.size() - 1).each {
                def k = word[it - n + 1..it].join('|')
                if (!ngrams.containsKey(k)) {
                    ngrams[k] = []
                }
                ngrams[k] << word[it + 1]
            }
            def k = word[-n..-1].join('|')
            if (!ngrams.containsKey(k)) {
                ngrams[k] = []
            }
            ngrams[k] << 'STOP'
        }

        def idx = 0
        new File(CONFIG.app.wordlist).eachLine { w ->
            if (w) {
                ngramize(w as List)
            }
            idx++
            if (idx % 1000000 == 0) {
                log.info("\t${idx} words processed.")
            }
        }
        new File(CONFIG.app.model).withObjectOutputStream { out ->
            out << ngrams
        }
        log.info("Done building the Markov chain.")
    }

    def buildMM() {
        log.info("Building Markov chain at ${CONFIG.app.model}.")

        ByteByteHashMap ngrams = new ByteByteHashMap(100000000)

        def ngramize = { List<String> word ->
            def n = [word.size(), CONFIG.app.window].min()
            (-n..-1).each {
                def k = (['START'] * (-it) + word[0..<n + it]).join('|').getBytes()
                if (!ngrams.containsKey(k)) {
                    ngrams[k] = []
                }
                ngrams[k] << word[n + it].getBytes()
            }
            (n - 1..<word.size() - 1).each {
                def k = word[it - n + 1..it].join('|').getBytes()
                if (!ngrams.containsKey(k)) {
                    ngrams[k] = []
                }
                ngrams[k] << word[it + 1].getBytes()
            }
            def k = word[-n..-1].join('|').getBytes()
            if (!ngrams.containsKey(k)) {
                ngrams[k] = []
            }
            ngrams[k] << 'STOP'.getBytes()
        }

        def idx = 0
        new File(CONFIG.app.wordlist).eachLine { w ->
            if (w) {
                ngramize(w as List)
            }
            idx++
            if (idx % 1000000 == 0) {
                log.info("\t${idx} words processed.")
            }
        }
        new File(CONFIG.app.model).withObjectOutputStream { out ->
            out << ngrams
        }
        log.info("Done building the Markov chain.")
    }

    def makeNonword0() {
        if (!MODEL) {
            new File(CONFIG.app.model).withObjectInputStream { is ->
                MODEL = is.readObject()
            }
        }
        if (!WORDLIST) {
            WORDLIST = new File(CONFIG.app.wordlist).readLines() as Set
        }

        ArrayList.metaClass.getRand = {
            return delegate[new Random().nextInt(delegate.size)]
        }

        def generate = {
            def context = (['START'] * CONFIG.app.window) as Queue
            def w = ''
            while (true) {
                def next = MODEL[context].getRand()
                if (next == 'STOP') {
                    break
                }
                w += next
                context.remove()
                context.add(next)
            }
            return w
        }

        def nonword
        while (true) {
            nonword = generate()
            if (!WORDLIST.contains(nonword)) {
                break
            }
        }
        return nonword
    }

    def makeNonword() {
        if (!MODEL) {
            new File(CONFIG.app.model).withObjectInputStream { is ->
                MODEL = is.readObject()
            }
        }
        if (!WORDLIST) {
            WORDLIST = new File(CONFIG.app.wordlist).readLines() as Set
        }

        ArrayList.metaClass.getRand = {
            return delegate[new Random().nextInt(delegate.size)]
        }

        def generate = {
            def context = (['START'] * CONFIG.app.window) as Queue
            def w = ''
            while (true) {
                def k = context.join('|').getBytes()
                def v = MODEL[k]
                def next = MODEL[k].getRand()
                if (next == 'STOP') {
                    break
                }
                w += next
                context.remove()
                context.add(next)
            }
            return w
        }

        def nonword
        while (true) {
            nonword = generate()
            if (!WORDLIST.contains(nonword)) {
                break
            }
        }
        return nonword
    }

    public static void main(String[] args) {
        def app = new App()
        app.CONFIG = new ConfigSlurper().parse(App.class.getResource("/app.config"))

//        app.dumpWordlist()
        app.buildMM()
//        10.times {
//            println app.makeNonword()
//        }
    }

}