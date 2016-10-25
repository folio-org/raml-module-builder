package org.folio.rest.tools.utils;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import com.google.common.reflect.ClassPath;


public class InterfaceToImpl {
  
  //we look for the class and function in the class that is mapped to a requested url
  //since we try to load via reflection an implementation of the class at runtime - better to load once and cache
  //for subsequent calls
  private static Table<String, String, ArrayList<Class<?>>> clazzCache  = HashBasedTable.create();
  private static final Logger log                                       = LoggerFactory.getLogger(InterfaceToImpl.class);

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
      log.debug("returned " +cachedClazz.size()+" class/es from cache");
      return cachedClazz;
    }
    ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
    ImmutableSet<ClassPath.ClassInfo> classes = classPath.getTopLevelClasses(implDir);
    for (ClassPath.ClassInfo info : classes) {
      try {
        Class<?> clazz = Class.forName(info.getName());
        for (Class<?> anInterface : clazz.getInterfaces()) {
          if (!anInterface.getName().equals(interface2check)) {
            continue;
          }
          if (!allowMultiple && impl.size() > 0) {
            throw new RuntimeException("Duplicate implementation of " + interface2check + " in " + implDir + ": " + impl.get(0).getName() + ", "
                + clazz.getName());
          }
          impl.add(clazz);
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
