package ch.cern.utils;

import java.io.Serializable;
import java.util.Arrays;

public class ByteArray implements Serializable {

	private static final long serialVersionUID = -337388419815576498L;
	
	private byte[] values;

	public ByteArray(byte[] values) {
		super();
		this.values = values;
	}

	public byte[] get() {
		return values;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(values);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteArray other = (ByteArray) obj;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return Arrays.toString(values);
	}
	
}
