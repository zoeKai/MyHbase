package org.myhbase.domain;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class ReflectClass {
	
	@Getter @Setter private String name;
	@Getter @Setter private List<ReflectField> fields = new ArrayList<ReflectField>();
	@Getter @Setter private ReflectMethodRowKey rowKeyMethod;
	@Getter @Setter private ReflectMethodTimestamp timestampMethod;
}
