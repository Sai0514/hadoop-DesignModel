package org.apache.lrn.DesignPartern02;

public class DefinedSort implements Comparable<Object>{
	private String user;
	private String spname;
	private long sumload;  	
	
	public DefinedSort(){
		super();
	}
	public DefinedSort(String user, String spname, long sumload) {
		super();
		this.user = user;
		this.spname = spname;
		this.sumload = sumload;
	}
	
	public String getU() {
		return user;
	}
	public void setU(String user) {
		this.user = user;
	}
	public String getP() {
		return spname;
	}
	public void setP(String spname) {
		this.spname = spname;
	}
	public long getSum() {
		return sumload;
	}
	public void setSum(long sumload) {
		this.sumload = sumload;
	}

	int flag=1;
	@Override
	public int compareTo(Object o) {
		DefinedSort ds=(DefinedSort)o;
		if(this.sumload<ds.sumload){
			return 1;
		}
		if(this.sumload>ds.sumload){
			return -1;			
		}
		flag=0;
		return 0;
	}
	
	@Override
	public String toString() {
		if(flag==0){
			return null;		 
		}else{
		 return user + "\t" + spname + "\t" + sumload ;
		}
	}	
}

