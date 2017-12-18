package ch.cern.spark.status;

import ch.cern.spark.status.storage.ClassNameAlias;

@ClassNameAlias("test-status")
public class TestStatus extends StatusValue {

	private static final long serialVersionUID = 2051099937848011407L;
	
	private long number = 10;

	public TestStatus(int i) {
		number = i;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (number ^ (number >>> 32));
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
		TestStatus other = (TestStatus) obj;
		if (number != other.number)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TestStatus [number=" + number + "]";
	}

}