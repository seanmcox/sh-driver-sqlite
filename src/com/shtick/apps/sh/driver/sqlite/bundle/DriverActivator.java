package com.shtick.apps.sh.driver.sqlite.bundle;

import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import com.shtick.apps.sh.core.Driver;
import com.shtick.apps.sh.driver.sqlite.SQLiteDriver;

/**
 **/
public class DriverActivator implements BundleActivator {
	private ServiceRegistration<?> driverRegistration;
	/**
	 * A source for registered subject question generators.
	 */
	public static SubjectQuestionGeneratorTracker SUBJECT_QUESTION_GENERATOR_TRACKER;
	
    /**
     * Implements BundleActivator.start(). Prints
     * a message and adds itself to the bundle context as a service
     * listener.
     * @param context the framework context for the bundle.
     **/
    @Override
	public void start(BundleContext context){
		System.out.println(this.getClass().getCanonicalName()+": Starting.");
		SUBJECT_QUESTION_GENERATOR_TRACKER = new SubjectQuestionGeneratorTracker(context);
		driverRegistration=context.registerService(Driver.class.getName(), new SQLiteDriver(),new Hashtable<String, String>());
    }

    /**
     * Implements BundleActivator.stop(). Prints
     * a message and removes itself from the bundle context as a
     * service listener.
     * @param context the framework context for the bundle.
     **/
    @Override
	public void stop(BundleContext context){
		System.out.println(this.getClass().getCanonicalName()+": Stopping.");
		SUBJECT_QUESTION_GENERATOR_TRACKER = null;
		if(driverRegistration!=null)
			driverRegistration.unregister();
    }

}