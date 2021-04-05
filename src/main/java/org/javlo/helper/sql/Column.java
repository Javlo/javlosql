package org.javlo.helper.sql;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
	String name() default "";

	String type() default "";
	
	boolean notNull() default false;
	
	boolean primaryKey() default false;
	
	boolean auto() default false;
	
	boolean unique() default false;
	
	String foreign() default "";
	
	String defaultValue() default "";
}