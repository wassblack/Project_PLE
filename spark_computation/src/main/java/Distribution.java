import java.util.List;

import scala.Tuple2;

public class Distribution
{
	private double min;
	private double max;
	private double avg;
	
	private long median;
	private long firstQuartile;
	private long thirdQuartile;
	
	private Tuple2<double[], long[]> histogram;
	
	public Distribution() {}
	
	public void fillOutput(List<String> output) {
		output.add("Min : " + min);
		output.add("Max : " + max);
		output.add("Avg : " + avg);
		output.add("Median : " + median);
		output.add("First quartile : " + firstQuartile);
		output.add("Third quartile : " + thirdQuartile);
		
		long total = 0;
		for (int i = 0; i < histogram._2.length; i++) {
			total += histogram._2[i];
		}
		
		for (int i = 0; i < histogram._1.length - 1; i++) {
			StringBuilder histogramString = new StringBuilder();

			histogramString.append("[" + histogram._1[i] + ", " + histogram._1[i+1]);
			if (i == histogram._1.length - 2) {
				histogramString.append("] : ");
			}
			else {
				histogramString.append(") : ");
			}
			histogramString.append(histogram._2[i]);
			double percentage = histogram._2[i] / (double) total * 100;
			histogramString.append(" (" + percentage + "%)");

			output.add(histogramString.toString());
		}
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public long getMedian() {
		return median;
	}

	public void setMedian(long median) {
		this.median = median;
	}

	public long getFirstQuartile() {
		return firstQuartile;
	}

	public void setFirstQuartile(long firstQuartile) {
		this.firstQuartile = firstQuartile;
	}

	public long getThirdQuartile() {
		return thirdQuartile;
	}

	public void setThirdQuartile(long thirdQuartile) {
		this.thirdQuartile = thirdQuartile;
	}

	public Tuple2<double[], long[]> getHistogram() {
		return histogram;
	}

	public void setHistogram(Tuple2<double[], long[]> histogram) {
		this.histogram = histogram;
	}
	
	
}
