/**
 * 
 */
package com.shtick.apps.sh.driver.sqlite.bundle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;

import com.shtick.apps.sh.core.Subject;
import com.shtick.apps.sh.core.SubjectQuestionGenerator;

/**
 * @author sean.cox
 *
 */
public class SubjectQuestionGeneratorTracker implements ServiceListener{
	/**
	 * The text that indicates a null data type when associating data types with apps.
	 * the null data type means that the app does not require data to be opened.
	 */
	public static final String DATA_TYPE_NULL="null";
	private HashMap<Subject,ServiceReference<SubjectQuestionGenerator>> subjectQuestionGenerators=new HashMap<>();
	private BundleContext bundleContext;

	/**
	 * @param bundleContext
	 */
	public SubjectQuestionGeneratorTracker(BundleContext bundleContext) {
		super();
		this.bundleContext = bundleContext;
		try{
			synchronized(subjectQuestionGenerators){
				bundleContext.addServiceListener(this, "(objectClass=com.shtick.apps.sh.core.SubjectQuestionGenerator)");
				ServiceReference<?>[] references=bundleContext.getServiceReferences(SubjectQuestionGenerator.class.getName(), null);
				if(references!=null){
					for(ServiceReference<?> ref:references){
						try{
							registerSubjectQuestionGenerator(ref);
						}
						catch(AbstractMethodError t){
							Object service=bundleContext.getService(ref);
							System.err.println(service.getClass().getCanonicalName());
							System.err.flush();
							t.printStackTrace();
						}
					}
				}
			}
		}
		catch(InvalidSyntaxException t){
			throw new RuntimeException(t);
		}
	}
	
	/**
	 * 
	 * @param subject
	 * @return The SubjectQuestionGenerator for the given subject, or null if none is known.
	 */
	public SubjectQuestionGenerator getSubjectQuestionGenerator(Subject subject){
		ServiceReference<?> reference = subjectQuestionGenerators.get(subject);
		if(reference==null)
			return null;
		return (SubjectQuestionGenerator)bundleContext.getService(reference);
	}
	
	/**
	 * 
	 * @return A set of already registered appFactories.
	 */
	public Set<SubjectQuestionGenerator> getSubjectQuestionGenerators(){
		HashSet<SubjectQuestionGenerator> retval=new HashSet<>();
		synchronized(subjectQuestionGenerators){
			SubjectQuestionGenerator service;
			for(ServiceReference<?> reference:subjectQuestionGenerators.values()){
				service=(SubjectQuestionGenerator)bundleContext.getService(reference);
				retval.add(service);
			}
		}
		return retval;
	}
	
	/**
	 * The caller of this method should be synchronized on the appFactoryServices object.
	 * 
	 * @param ref
	 * @throws AbstractMethodError If the AppFactoryService is not compatible with this implementation of the AppTracker sufficient to be registered.
	 */
	private void registerSubjectQuestionGenerator(ServiceReference<?> ref) throws AbstractMethodError{
		Object service=bundleContext.getService(ref);
		if(!(service instanceof SubjectQuestionGenerator))
			return;
		SubjectQuestionGenerator subjectQuestionGenerator=(SubjectQuestionGenerator)service;
		Subject subjectName = subjectQuestionGenerator.getSubject();
		if(!subjectQuestionGenerators.containsKey(subjectName))
			subjectQuestionGenerators.put(subjectName,(ServiceReference<SubjectQuestionGenerator>)ref);
		else
			System.err.println("Attempted to register duplicate subject: "+subjectName);
	}

	/**
	 * The caller of this method should be synchronized on the appFactoryServices object.
	 * 
	 * @param ref
	 */
	private void unregisterSubjectQuestionGenerator(ServiceReference<?> ref){
		Object service=bundleContext.getService(ref);
		if(!(service instanceof SubjectQuestionGenerator))
			return;
		SubjectQuestionGenerator appFactoryService=(SubjectQuestionGenerator)service;
		subjectQuestionGenerators.remove(service.getClass().getCanonicalName());
	}

	@Override
	public void serviceChanged(ServiceEvent event) {
		synchronized(subjectQuestionGenerators){
			if(event.getType() == ServiceEvent.REGISTERED){
				ServiceReference<?> ref=event.getServiceReference();
				registerSubjectQuestionGenerator(ref);
			}
			else if(event.getType() == ServiceEvent.UNREGISTERING){
				ServiceReference<?> ref=event.getServiceReference();
				unregisterSubjectQuestionGenerator(ref);
			}
		}
	}
}
