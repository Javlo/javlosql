package org.javlo.helper.sql;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Label {
	String label() default "";
	String key() default "";
}