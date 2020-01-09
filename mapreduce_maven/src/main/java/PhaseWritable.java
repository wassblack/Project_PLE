

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
    //private int npatterns;
    private String jobs;
    //private int njobs;
    private String days;
    //private int ndays;

    public PhaseWritable() { }

    public Long getDuration() {
        return duration;
    }

    public String getPatterns() {
        return patterns;
    }

    public int getNpatterns() {
        //return npatterns;
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
        //return njobs;
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
        //return ndays;
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
/*
    public void setNpatterns(int npatterns) {
        this.npatterns = npatterns;
    }
*/
    public void setJobs(String jobs) {
        this.jobs = jobs;
    }
/*
    public void setNjobs(int njobs) {
        this.njobs = njobs;
    }
*/
    public void setDays(String days) {
        this.days = days;
    }
/*
    public void setNdays(int ndays) {
        this.ndays = ndays;
    }
*/
    public void write(DataOutput out) throws IOException {
        out.writeLong(duration);
        out.writeUTF(patterns);
        //out.writeInt(npatterns);
        out.writeUTF(jobs);
        //out.writeInt(njobs);
        out.writeUTF(days);
        //out.writeInt(ndays);
    }

    public void readFields(DataInput in) throws IOException {
        duration = in.readLong();
        patterns = in.readUTF();
        //npatterns = in.readInt();
        jobs = in.readUTF();
        //njobs = in.readInt();
        days = in.readUTF();
        //ndays = in.readInt();
    }


}
