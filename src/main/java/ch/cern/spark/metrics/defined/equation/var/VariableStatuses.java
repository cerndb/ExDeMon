package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;

@ClassNameAlias("defined-metric-status")
public class VariableStatuses extends StatusValue {
	
	private static final long serialVersionUID = 3020679839103994736L;

	private Map<String, StatusValue> statuses;
	
	private Instant processedBatchTime;
	
	public VariableStatuses() {
		statuses = new HashMap<>();
	}

	public boolean newProcessedBatchTime(Instant time) {
		if(this.processedBatchTime == null) {
			this.processedBatchTime = time;
			
			return true;
		}else {
			if(this.processedBatchTime.compareTo(time) == 0) {
				return false;
			}else{
				this.processedBatchTime = time;
				
				return true;
			}
		}
	}

	@Override
	public String toString() {
		return "VariableStatuses [statuses=" + statuses + ", processedBatchTime=" + processedBatchTime + "]";
	}

	public Instant getProcessedBatchTime() {
		return processedBatchTime;
	}

	public void put(String varName, StatusValue status) {
		statuses.put(varName, status);
	}

	public boolean containsVariable(String StatusValue) {
		return statuses.containsKey(StatusValue);
	}

	public StatusValue get(String varName) {
		return statuses.get(varName);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((processedBatchTime == null) ? 0 : processedBatchTime.hashCode());
		result = prime * result + ((statuses == null) ? 0 : statuses.hashCode());
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
		VariableStatuses other = (VariableStatuses) obj;
		if (processedBatchTime == null) {
			if (other.processedBatchTime != null)
				return false;
		} else if (!processedBatchTime.equals(other.processedBatchTime))
			return false;
		if (statuses == null) {
			if (other.statuses != null)
				return false;
		} else if (!statuses.equals(other.statuses))
			return false;
		return true;
	}
	
}
