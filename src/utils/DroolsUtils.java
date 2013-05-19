package utils;

import java.io.File;

import org.kie.api.io.ResourceType;

import org.kie.internal.KnowledgeBase;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;

/*
 * A utility class that wraps the basic functionality of Drools rules.
 */
public class DroolsUtils {
   /** 
    * Create new knowledge base 
    */  
	public static KnowledgeBase createKnowledgeBase(String drlFileLoc) 
	{  
	    KnowledgeBuilder builder = KnowledgeBuilderFactory.newKnowledgeBuilder();  
	            //Add drl file into builder  
	    File accountRules = new File(drlFileLoc); //Where the account rule is.  
	    builder.add(ResourceFactory.newFileResource(accountRules), ResourceType.DRL);  
	    if (builder.hasErrors()) {  
	        throw new RuntimeException(builder.getErrors().toString());  
	    }  
	
	    KnowledgeBase knowledgeBase = KnowledgeBaseFactory.newKnowledgeBase();
	
        //Add to Knowledge Base packages from the builder which are actually the rules from the drl file.  
	    knowledgeBase.addKnowledgePackages(builder.getKnowledgePackages());  
	
	    return knowledgeBase;
	}

}