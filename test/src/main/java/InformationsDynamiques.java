import java.util.Arrays;
import java.util.Date;

public class InformationsDynamiques {
	private double cap;
	private boolean clignotant;
	private Date date;
	private boolean freine;
	private boolean gyrophare;
	private double[] lonlat;
	private Phare phare;
	private double vitesseAbsolue;
	public InformationsDynamiques(double cap, boolean clignotant, boolean freine, boolean gyrophare,
			double[] lonlat, Phare phare, double vitesseAbsolue) {
		super();
		this.cap = cap;
		this.clignotant = clignotant;
		this.date = new Date();
		this.freine = freine;
		this.gyrophare = gyrophare;
		this.lonlat = lonlat;
		this.phare = phare;
		this.vitesseAbsolue = vitesseAbsolue;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(cap);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (clignotant ? 1231 : 1237);
		result = prime * result + (freine ? 1231 : 1237);
		result = prime * result + (gyrophare ? 1231 : 1237);
		result = prime * result + Arrays.hashCode(lonlat);
		result = prime * result + ((phare == null) ? 0 : phare.hashCode());
		temp = Double.doubleToLongBits(vitesseAbsolue);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		InformationsDynamiques other = (InformationsDynamiques) obj;
		if (Double.doubleToLongBits(cap) != Double.doubleToLongBits(other.cap))
			return false;
		if (clignotant != other.clignotant)
			return false;
		if (freine != other.freine)
			return false;
		if (gyrophare != other.gyrophare)
			return false;
		if (!Arrays.equals(lonlat, other.lonlat))
			return false;
		if (phare != other.phare)
			return false;
		if (Double.doubleToLongBits(vitesseAbsolue) != Double.doubleToLongBits(other.vitesseAbsolue))
			return false;
		return true;
	}
	
}
