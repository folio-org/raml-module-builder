package org.folio.rest.tools.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.reflect.ClassPath;


public class InterfaceToImpl {

  //we look for the class and function in the class that is mapped to a requested url
  //since we try to load via reflection an implementation of the class at runtime - better to load once and cache
  //for subsequent calls
  private static Table<String, String, ArrayList<Class<?>>> clazzCache  = HashBasedTable.create();
  private static final Logger log = LogManager.getLogger(InterfaceToImpl.class);

  /**
   * Return the implementing class.
   *
   * @param implDir
   *          - package name where to search
   * @param interface2check
   *          - class name of the required interface
   * @return implementing class/es
   * @throws IOException
   *           - if the attempt to read class path resources (jar files or directories) failed.
   * @throws ClassNotFoundException
   *           - if no class in implDir implements the interface
   */
  public static ArrayList<Class<?>> convert2Impl(String implDir, String interface2check, boolean allowMultiple) throws IOException, ClassNotFoundException {
    ArrayList<Class<?>> impl = new ArrayList<>();
    ArrayList<Class<?>> cachedClazz = clazzCache.get(implDir, interface2check);
    if(cachedClazz != null){
      log.debug("returned {} class/es from cache", cachedClazz.size());
      return cachedClazz;
    }

    ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
    Set<ClassPath.ClassInfo> classes = classPath.getTopLevelClasses(implDir);

    Class<?> userImpl = null;
    /** iterate over all classes in the org.folio.rest.impl package to find the one implementing the
     * requested interface */
    for (ClassPath.ClassInfo info : classes) {
      if(userImpl != null && impl.size() == 1){
        /** we found a user impl that matches the interface2check, we are done, since we can only have one of these */
        break;
      }
      try {
        Class<?> clazz = Class.forName(info.getName());
        if(!clazz.getSuperclass().getName().equals("java.lang.Object") && clazz.getSuperclass().getInterfaces().length > 0){ //NOSONAR
          /** user defined class which overrides one of the out of the box RMB implementations
           * set the clazz to the interface. find the correct implementation below */
          userImpl = clazz;
          clazz = clazz.getSuperclass();
        }
        /** loop over all interfaces the class implements */
        for (Class<?> anInterface : clazz.getInterfaces()) {
          if (!anInterface.getName().equals(interface2check)) { //NOSONAR
            /**if userImpl != null here then a user impl exists but does not match the requested interface, so set to null*/
            userImpl = null;
            /** class doesnt implement the requested package, continue to check if it implements other packages */
            continue;
          }
          if(userImpl != null){
            /** we are here if we found a user impl (extends) of an RMB existing interface implementation
             *  load the user implementation and remove previous impl if it was added in previous loop iterations
             * there can only be one impl of an override, but we dont know the order we will get from the classloader
             * will the default RMB impl be passed in here or the user implementation - so once we hit a class whose
             * super class extends a generated interface - clear out the previous impl if any from the array and add
             * the user's impl - once found we are done */
            impl.clear();
            impl.add(userImpl);
            break;
          }
          else if (!allowMultiple && impl.size() > 0) {
            throw new RuntimeException("Duplicate implementation of " + interface2check + " in " + implDir + ": " + impl.get(0).getName() + ", "
                + clazz.getName());
          }
          else{
            /** return the class found that implements the requested package */
            impl.add(clazz);
          }
        }
      } catch (ClassNotFoundException e) {
        log.error(e.getMessage(), e);
      }
    }
    if (impl.isEmpty()) {
      throw new ClassNotFoundException("Implementation of " + interface2check + " not found in " + implDir);
    }
    clazzCache.put(implDir, interface2check, impl);
    return impl;
  }

}
