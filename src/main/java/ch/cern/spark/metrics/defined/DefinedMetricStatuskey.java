package ch.cern.spark.metrics.defined;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.JSONSerializationClassNameAlias;

@JSONSerializationClassNameAlias("defined-metric-key")
public class DefinedMetricStatuskey implements IDStatusKey {

	private static final long serialVersionUID = 2992810464329306975L;

	private String id;
	
	private Map<String, String> metric_ids;

	public DefinedMetricStatuskey(String definedMetricId, Map<String, String> metric_ids) {
		this.id = definedMetricId;
		this.metric_ids = metric_ids;
	}

	public String getID() {
		return id;
	}

	public Map<String, String> getGroupByMetricIDs() {
		return metric_ids;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((metric_ids == null) ? 0 : metric_ids.hashCode());
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
		DefinedMetricStatuskey other = (DefinedMetricStatuskey) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (metric_ids == null) {
			if (other.metric_ids != null)
				return false;
		} else if (!metric_ids.equals(other.metric_ids))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DefinedMetricID [name=" + id + ", metric_ids=" + metric_ids + "]";
	}
	
}
