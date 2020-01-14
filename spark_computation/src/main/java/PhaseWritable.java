

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Writable;

import scala.Array;

public class PhaseWritable implements Writable, Serializable
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Long start;
	private Long duration;
    private String patterns;
    private String jobs;
    private String days;

    public PhaseWritable() { }
    
    public List<Long> getPlagesHoraires()
    {
    	ArrayList<Long> plagesHoraires = new ArrayList<Long>();
    	
    	Date startDate = new Date(start);
    	Date endDate = new Date(this.getEnd());
    	
    	int startPlage = startDate.getHours();
    	int endPlage = endDate.getHours();
    	
    	int startDay = startDate.getDay();
    	int endDay = endDate.getDate();
    	
    	// si la phase dure plus de 24 heures
    	if (endDay - startDay > 1) {
    		for (long i = 0; i < 24; i++) {
	    		plagesHoraires.add(i);
	    	}
    	}
    	
    	else {
	    	// si la phase se passe entre 0h et 23h59
	    	if (startPlage <= endPlage) {
		    	for (long i = startPlage; i <= endPlage; i++) {
		    		plagesHoraires.add(i);
		    	}
	    	}
	    	
	    	// si la phase s'étale sur 2 jours
	    	else {
	    		System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  "  + startPlage + ", " + endPlage);
	    		
	    		for (long i = startPlage; i < 24; i++) {
	    			plagesHoraires.add(i);
	    		}
	    		for (long i = 0; i <= endPlage; i++) {
	    			plagesHoraires.add(i);
	    		}
	    	}
    	}
    	
    	if (plagesHoraires.isEmpty()) {
    		System.out.println("start: " + startPlage);
    		System.out.println("end: " + endPlage);
    	}
    	
    	return plagesHoraires;
    }
    
    public boolean onePatternIsPresent(String[] patterns) {
    	boolean isPresent = false;
    	
    	for (int i = 0; i < patterns.length; i++) {
    		if (patternIsPresent(patterns[i])) {
    			isPresent = true;
    			break;
    		}
    	}
    	
    	return isPresent;
    }
    
    public boolean patternsArePresents(String[] patterns) {
    	boolean arePresent = true;
    	
    	for (int i = 0; i < patterns.length; i++) {
    		if (!patternIsPresent(patterns[i])) {
    			arePresent = false;
    			break;
    		}
    	}
    	
    	return arePresent;
    }
    
    public boolean patternIsPresent(String pattern) {
    	boolean isPresent = false;
    	
    	for (String p : patterns.split(",")) {
    		if (p.equals(pattern)) {
    			isPresent = true;
    			break;
    		}
    	}
    	return isPresent;
    }
    
    public boolean isIdle() {
    	return patterns.equals("-1");
    }
    
    public Long getStart() {
    	return start;
    }
    
    public Long getEnd() {
    	return start + duration;
    }

    public Long getDuration() {
        return duration;
    }

    public String getPatterns() {
        return patterns;
    }

    public long getNpatterns() {
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

    public long getNjobs() {
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

    public long getNdays() {
    	if (days.equals("-1")) {
    		return 0;
    	}
    	else {
    		return days.split(",").length;
    	}
    }
    
    public void setStart(Long start) {
    	this.start = start;
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
    	out.writeLong(start);
        out.writeLong(duration);
        out.writeUTF(patterns);
        out.writeUTF(jobs);
        out.writeUTF(days);
    }

    public void readFields(DataInput in) throws IOException {
    	start = in.readLong();
        duration = in.readLong();
        patterns = in.readUTF();
        jobs = in.readUTF();
        days = in.readUTF();
    }

}
