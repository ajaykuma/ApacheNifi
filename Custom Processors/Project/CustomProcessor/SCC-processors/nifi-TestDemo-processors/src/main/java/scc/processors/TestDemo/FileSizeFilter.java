package scc.processors.TestDemo;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FileSizeFilter extends AbstractProcessor {
	
	private static int MAX_FILE_SIZE=0;
 
    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("pass the file")
            .description("this file will be passed on to success")
            .build();
    
    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("do not pass the file")
            .description("this file will not be passed on to success")
            .build(); 
    
    //adding a new property
    public static final PropertyDescriptor MAX_FILE_SIZE_ATTRIBUTE_PROPERTY = new PropertyDescriptor
            .Builder().name("Max File Size Attribute")
            .description("name of attribute that contains the file size")
            .required(true)
            .defaultValue("some.file.size")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MAX_FILE_SIZE_ATTRIBUTE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);
   //adding relationships
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(FAIL_RELATIONSHIP);
        relationships.add(SUCCESS_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}

        // TODO implement
		String attributeMax = context.getProperty(MAX_FILE_SIZE_ATTRIBUTE_PROPERTY).getValue();
	
	getLogger().info("Got a flow file");
	
	if (flowFile.getAttribute(attributeMax)!=null)
	{
			MAX_FILE_SIZE=Integer.parseInt(flowFile.getAttribute(attributeMax));
			getLogger().info("Reset Max file size value to "+MAX_FILE_SIZE+" Attrib Ref:"+attributeMax);
			session.remove(flowFile);
			return;
	}
	if (flowFile.getSize()<MAX_FILE_SIZE)
	{
		getLogger().info("File passed,size is less than "+MAX_FILE_SIZE+ "("+flowFile.getSize()+" )");	
		session.transfer(flowFile,SUCCESS_RELATIONSHIP);
	}
	else
	{
		getLogger().info("File not passed,size is more than "+MAX_FILE_SIZE+ "("+flowFile.getSize()+" )");	
		session.transfer(flowFile,FAIL_RELATIONSHIP);
	}
    }

}

