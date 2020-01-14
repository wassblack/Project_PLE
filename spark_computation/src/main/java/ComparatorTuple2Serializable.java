import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class ComparatorTuple2Serializable implements Comparator<Tuple2<String, Long>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
		return o1._2().compareTo(o2._2());
	}

}
