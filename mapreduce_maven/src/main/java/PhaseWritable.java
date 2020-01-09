

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class PhaseWritable implements Writable, Serializable{


    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long duration;
    private String patterns;
    private String jobs;
    private String days;

    public PhaseWritable() { }

    public Long getDuration() {
        return duration;
    }

    public String getPatterns() {
        return patterns;
    }

    public int getNpatterns() {
    	if (patterns.equals("-1")) {
    		return 0;
    	}
    	else {
    		return patterns.split(",").length;
    	}
    }

    public String getJobs() {
        return jobs;
    }

    public int getNjobs() {
    	if (jobs.equals("-1")) {
    		return 0;
    	}
    	else {
    		return jobs.split(",").length;
    	}
    }

    public String getDays() {
        return days;
    }

    public int getNdays() {
    	if (days.equals("-1")) {
    		return 0;
    	}
    	else {
    		return days.split(",").length;
    	}
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public void setPatterns(String patterns) {
        this.patterns = patterns;
    }

    public void setJobs(String jobs) {
        this.jobs = jobs;
    }

    public void setDays(String days) {
        this.days = days;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(duration);
        out.writeUTF(patterns);
        out.writeUTF(jobs);
        out.writeUTF(days);
    }

    public void readFields(DataInput in) throws IOException {
        duration = in.readLong();
        patterns = in.readUTF();
        jobs = in.readUTF();
        days = in.readUTF();
    }


}
