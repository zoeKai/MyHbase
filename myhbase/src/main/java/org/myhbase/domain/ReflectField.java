package org.myhbase.domain;

import java.lang.reflect.Field;

import org.myhbase.annotation.HBaseColumn;

import lombok.Getter;
import lombok.Setter;

public class ReflectField {

	@Getter @Setter private Field field;
	@Getter @Setter private HBaseColumn hBaseColumn;
}
