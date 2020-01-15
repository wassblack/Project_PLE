

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
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
    	
    	Instant startIns = Instant.ofEpochMilli(start);
    	Instant endIns = Instant.ofEpochMilli(this.getEnd());
    
    	int startPlage = (int) startIns.atZone(ZoneOffset.UTC).getHour();
    	int endPlage = (int) endIns.atZone(ZoneOffset.UTC).getHour();
    	
    	Duration duration = Duration.between(startIns, endIns);
    	
    	// si la phase dure 24 heures ou plus
    	if (duration.toHours() >= 24) {
    		//System.out.println("plus de 24 heures");
    		for (long i = 0; i < 24; i++) {
    			plagesHoraires.add(i);
	    	}
    	}
    	
    	else {
	    	// si la phase se passe entre la plage horaire 0 et 23
	    	if (startPlage <= endPlage) {
		    	for (long i = startPlage; i <= endPlage; i++) {
		    		plagesHoraires.add(i);
		    	}
	    	}
	    	
	    	// si la phase s'étale sur 2 jours
	    	else {
	    		for (long i = startPlage; i < 24; i++) {
	    			plagesHoraires.add(i);
	    		}
	    		for (long i = 0; i <= endPlage; i++) {
	    			plagesHoraires.add(i);
	    		}
	    	}
    	}
    	
    	return plagesHoraires;
    }
    
    public String getPlagesHorairesString()
    {
    	String plagesHoraires = "";
    	
    	Instant startIns = Instant.ofEpochMilli(start);
    	Instant endIns = Instant.ofEpochMilli(this.getEnd());
    
    	int startPlage = (int) startIns.atZone(ZoneOffset.UTC).getHour();
    	int endPlage = (int) endIns.atZone(ZoneOffset.UTC).getHour();
    	
    	Duration duration = Duration.between(startIns, endIns);
  
    	// si la phase dure 24 heures ou plus
    	if (duration.toHours() >= 24) {
    		//System.out.println("plus de 24 heures");
    		for (long i = 0; i < 24; i++) {
    			plagesHoraires += i + ",";
	    	}
    	}
    	
    	else {
	    	// si la phase se passe entre la plage horaire 0 et 23
	    	if (startPlage <= endPlage) {
	    		//System.out.println("entre 0 et 23");
		    	for (int i = startPlage; i <= endPlage; i++) {
		    		plagesHoraires += i + ",";
		    	}
	    	}
	    	
	    	// si la phase s'étale sur 2 jours
	    	else {
	    		//System.out.println("plus de 2 jours");
	    		for (int i = startPlage; i < 24; i++) {
	    			plagesHoraires += i + ",";
	    		}
	    		for (int i = 0; i <= endPlage; i++) {
	    			plagesHoraires += i + ",";
	    		}
	    	}
    	}
    	
    	return plagesHoraires.substring(0, plagesHoraires.length() - 1);
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
