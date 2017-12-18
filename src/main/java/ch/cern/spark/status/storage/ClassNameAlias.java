package ch.cern.spark.status.storage;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ClassNameAlias {

	String value();
	
}
