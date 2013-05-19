import hdpjobs.SetCartesianProduct.Citation;
import hdpjobs.SetCartesianProduct.CitationPair;

import org.kie.internal.KnowledgeBase;
import org.kie.internal.runtime.StatefulKnowledgeSession;

import utils.DroolsUtils;

public class Labs {

	public static void main(String[] args) {
		// Create KnowledgeBase...
		KnowledgeBase knowledgeBase = DroolsUtils.createKnowledgeBase("/home/hduser/workspace/EntityMatcher/src/rules/entityMatchNegRules.drl");
		// Create a statefull session
		StatefulKnowledgeSession session = knowledgeBase.newStatefulKnowledgeSession();
		try {
			Citation test1 = new Citation("123", "hbase1", null, "singapore", 1999);
			Citation test2 = new Citation("999", "hbase2", null, "singapore", 1999);
			CitationPair entityPair = new CitationPair(test1, test2);
			System.out.println("Firing the rules ..");
			session.insert(entityPair);
			session.fireAllRules();
			
			System.out.println("Matching candidates ? " + entityPair.areMatchingCandidates());
		} finally {
			session.dispose();
		}

	}

}
