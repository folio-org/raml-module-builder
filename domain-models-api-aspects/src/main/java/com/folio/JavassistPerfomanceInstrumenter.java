package com.folio;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.LoaderClassPath;
import javassist.NotFoundException;
import javassist.expr.ExprEditor;
import javassist.expr.MethodCall;
import static java.util.Arrays.asList;
/**
 * @author shale
 *
 */
public class JavassistPerfomanceInstrumenter {

  boolean isStatic = false;
  boolean isPublic = false;
  boolean isPrivate = false;
  boolean isProtected = false;
  List<Boolean> flags = new ArrayList<>();
  HashSet<String> functions = new HashSet<>();

  
  public void insertTimingIntoMethod() throws NotFoundException, CannotCompileException, IOException {
    
    String targetFolder = System.getProperty("complied_class_output_dir");
    String class2Get = System.getProperty("class_list");
    String handlerNames = System.getProperty("handler_function_names"); 

    // public,private,protected,static - allow in future list of methods - all modifiers will be with final and synchronized and without final, sync
    // not supporting native, transient, volatile, strict, abstract, interface, native modifiers
    String methods2inst = System.getProperty("methods_to_instrument"); 
    
    String []classList = class2Get.split(",");
    String []modifiers = methods2inst.split(",");
    String []asyncs    = handlerNames.split(",");
    
    
    for (String key : asyncs)
      functions.add(key);
    
    setModifiers(modifiers);
    
    Method[] methods;
    try {
      final ClassPool pool = ClassPool.getDefault();
      pool.appendClassPath(new LoaderClassPath(getClass().getClassLoader()));

      for (int i = 0; i < classList.length; i++) {
        //convert string name to ctclass
        final CtClass compiledClass = pool.get(classList[i]);
        
        
        CtMethod[] m = compiledClass.getDeclaredMethods();
        for (int j = 0; j < m.length; j++) {
          if(m[j].getName().contains("lambda")){
            try {
              m[j].insertAfter("{long __endMsGen__ = System.currentTimeMillis();" +
                        "System.out.println(\"Executed in ms: \" + (__endMsGen__-__startMsGen__));}");
/*              m[j].instrument(new ExprEditor() {
                public void edit(final MethodCall m) throws CannotCompileException {
                  //if ("java.lang.Integer".equals(m.getClassName()) && "max".equals(m.getMethodName())) {
                  
                  //scan all methods in the instrumented method to place the timer after the nested method 
                  //that was required
                  System.out.println("try adding " + m.getMethodName());
                  //if(functions.contains(m.getMethodName()) && "io.vertx.core.Handler" .equals(m.getClassName())){
                  //if("failed".contains(m.getMethodName()) || "succeeded".contains(m.getMethodName())){

                    m.replace("{ " +
                        "$_ = $proceed($$); " +
                        "long __endMsGen__ = System.currentTimeMillis();" +
                        "System.out.println(\"Executed in ms: \" + (__endMsGen__-__startMsGen__));}");
                 // }
                    
                  System.out.println("added to" + m.getMethodName());
                  //}
                }
              });*/
              System.out.println("added to" + m[j]);

            } catch (Exception e) {
              System.out.println("NOT added to 1 " + m[j].getLongName());

              //e.printStackTrace();
            }
          }
          else{
            try {
              m[j].addLocalVariable("__startMsGen__", CtClass.longType);
              m[j].insertBefore("__startMsGen__ = System.currentTimeMillis();");
              System.out.println("added to" + m[j].getLongName());
            } catch (Exception e) {
              // TODO Auto-generated catch block
              //e.printStackTrace();
              System.out.println("NOT added to" + m[j].getLongName());

            }    
          }
        }

/*        //get methods in class and filter out the ones to instrument
        methods = Class.forName(class2Get).getMethods();
        for (int j = 0; j < methods.length; j++) {
          int modifier = methods[j].getModifiers();
          if(flags.get(modifier)){
            final CtMethod method = compiledClass.getDeclaredMethod(methods[j].getName());
            //final CtMethod method = compiledClass.getDeclaredMethod("select");
            method.addLocalVariable("__startMsGen__", CtClass.longType);
            method.insertBefore("__startMsGen__ = System.currentTimeMillis();");
            method.getMethodInfo();
            method.getMethodInfo2();
            method.getAvailableParameterAnnotations();
            method.instrument(new ExprEditor() {
              public void edit(final MethodCall m) throws CannotCompileException {
                //if ("java.lang.Integer".equals(m.getClassName()) && "max".equals(m.getMethodName())) {
                
                //scan all methods in the instrumented method to place the timer after the nested method 
                //that was required
                System.out.println(m.getMethodName());
                if(functions.contains(m.getMethodName()) && "io.vertx.core.Handler" .equals(m.getClassName())){
                  m.replace("{ " +
                      "$_ = $proceed($$); " +
                      "long __endMsGen__ = System.currentTimeMillis();" +
                      "System.out.println(\"Executed in ms: \" + (__endMsGen__-__startMsGen__));}");
                }
              }
            });
          }          
        }*/
        compiledClass.writeFile(targetFolder);
      }
    } catch (Exception e1) {
      e1.printStackTrace();
    } 

/*    try {
       final ClassPool pool = ClassPool.getDefault();
       // Tell Javassist where to look for classes - into our ClassLoader
       pool.appendClassPath(new LoaderClassPath(getClass().getClassLoader()));
       //final CtClass compiledClass = pool.get(targetClass);
       //final CtMethod method = compiledClass.getDeclaredMethod(targetMethod);
       final CtClass compiledClass = pool.get(class2Get);
       final CtMethod method = compiledClass.getDeclaredMethod("avc");
       
       // Add something to the beginning of the method:
       method.addLocalVariable("startMs", CtClass.longType);
       method.insertBefore("startMs = System.currentTimeMillis();");
       
       method.instrument(new ExprEditor() {
         public void edit(final MethodCall m) throws CannotCompileException {
           if ("java.lang.Integer".equals(m.getClassName()) && "max".equals(m.getMethodName())) {
             m.replace("{ " +
                 "$_ = $proceed($$); " +
                 "long endMs = System.currentTimeMillis();" +
                 "System.out.println(\"Executed in ms: \" + (endMs-startMs));}");
           }
         }
       });
       

       // And also to its very end:
       method.insertAfter("{final long endMs = System.currentTimeMillis();" +
          "iterate.jz2011.codeinjection.javassist.PerformanceMonitor.logPerformance(\"" +
          targetMethod + "\",(endMs-startMs));}");

       method.insertAfter("{final long endMs = System.currentTimeMillis();" +
           "System.out.println(\"" +
           targetMethod + "\" + (endMs-startMs));}");
       
       compiledClass.writeFile(targetFolder);
       // Enjoy the new $targetFolder/iterate/jz2011/codeinjection/javassist/TargetClass.class

      // logger.info(targetClass + "." + targetMethod +
      //       " has been modified and saved under " + targetFolder);
    } catch (NotFoundException e) {
      e.printStackTrace();
      // logger.warning("Failed to find the target class to modify, " +
      //       targetClass + ", verify that it ClassPool has been configured to look " +
     //        "into the right location");
    }*/
 }

  /**
   * 
   * @param args
   * @throws Exception
   */
 public static void main(String[] args) throws Exception {
   
   
    System.setProperty("complied_class_output_dir", "./target/classes");
    System.setProperty("class_list", "com.sling.rest.persist.PostgresClient");
    System.setProperty("methods_to_instrument", "public");
    System.setProperty("handler_function_names", "handle"); 
    String targetFolder = System.getProperty("complied_class_output_dir");
    String class2Get = System.getProperty("class_list");
    String methods2inst = System.getProperty("methods_to_instrument");
    new JavassistPerfomanceInstrumenter().insertTimingIntoMethod();
    
    
    //new JavassistPerfomanceInstrumenter().avc();
 }
 
 public void avc(){
   
   System.out.println("this is a test....");
   Integer.max(1, 3);
   String.valueOf(7);


 }

 private void setModifiers(String []modifiers){
   for(int i=0; i<2048; i++){
     flags.add(i, Boolean.FALSE);
   }
   for (int i = 0; i < modifiers.length; i++) {
     if("public".equalsIgnoreCase(modifiers[i])){
       isPublic = true;
     }
     else if("private".equalsIgnoreCase(modifiers[i])){
       isPrivate = true;
     }
     else if("static".equalsIgnoreCase(modifiers[i])){
       isStatic = true;
     }
     else if("protected".equalsIgnoreCase(modifiers[i])){
       isProtected = true;
     }
   }
   
   if(isPublic){
     flags.set(1, true); // public modifer = 1  (https://docs.oracle.com/javase/7/docs/api/constant-values.html#java.lang.reflect.Modifier.ABSTRACT)
     flags.set(java.lang.reflect.Modifier.FINAL + 1, true); //also include final public methods
     flags.set(java.lang.reflect.Modifier.FINAL + java.lang.reflect.Modifier.SYNCHRONIZED + 1, true); 
     if(isStatic){
       flags.set(1 + 8, true); // this is a public static function 
       flags.set(java.lang.reflect.Modifier.FINAL + 1 + 8, true); // this is a final public static function 
       flags.set(java.lang.reflect.Modifier.FINAL + java.lang.reflect.Modifier.SYNCHRONIZED + 1 + 8, true); 

     }
   }
   else if(isPrivate){
     flags.set(2, true); // private modifer = 1 
     flags.set(2 + java.lang.reflect.Modifier.FINAL, true);
     flags.set(2 + java.lang.reflect.Modifier.FINAL + java.lang.reflect.Modifier.SYNCHRONIZED, true); 

     if(isStatic){
       flags.set(2 + 8, true); // this is a private static function 
       flags.set(2 + 8 + java.lang.reflect.Modifier.FINAL, true); // this is a final private static function 
       flags.set(java.lang.reflect.Modifier.FINAL + java.lang.reflect.Modifier.SYNCHRONIZED + 2 + 8, true); 
     }
   }
   else if(isProtected){
     flags.set(4, true);
     flags.set(4 + java.lang.reflect.Modifier.FINAL, true);
     flags.set(java.lang.reflect.Modifier.FINAL + java.lang.reflect.Modifier.SYNCHRONIZED + 4, true); 
     if(isStatic){
       flags.set(4 + 8, true); // this is a protected static function 
       flags.set(4 + 8 + java.lang.reflect.Modifier.FINAL, true); // this is a final protected static function 
       flags.set(java.lang.reflect.Modifier.FINAL + java.lang.reflect.Modifier.SYNCHRONIZED + 4 + 8, true); 
     }
   }
 }
 
}
