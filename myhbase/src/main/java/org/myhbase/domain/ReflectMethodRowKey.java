package org.myhbase.domain;

import org.myhbase.annotation.HBaseRowKey;

import lombok.Getter;
import lombok.Setter;

public class ReflectMethodRowKey extends ReflectMethod {
	
	@Getter @Setter private HBaseRowKey hBaseRowKey;

}
