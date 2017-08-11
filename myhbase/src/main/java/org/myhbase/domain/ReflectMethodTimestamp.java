package org.myhbase.domain;

import org.myhbase.annotation.HBaseTimestamp;

import lombok.Getter;
import lombok.Setter;

public class ReflectMethodTimestamp extends ReflectMethod {
	
	@Getter @Setter private HBaseTimestamp hBaseTimestamp;

}
