import java.util.ArrayList;
import java.util.List;

public class Vehicule {
	private int couleur;
	private double hauteur;
	private String immatriculation;
	private double largeur;
	private double longueur;
	private String marque;
	private String modele;
	private boolean remorque;
	private TypeVehicule type;
	private TypePrioritaire typeprioritaire;
	private List<InformationsDynamiques> informationsDynamiques = new ArrayList<>();
	
	public Vehicule(int couleur, double hauteur, String immatriculation, double largeur, double longueur, String marque,
			String modele, boolean remorque, TypeVehicule type, TypePrioritaire typeprioritaire) {
		super();
		this.couleur = couleur;
		this.hauteur = hauteur;
		this.immatriculation = immatriculation;
		this.largeur = largeur;
		this.longueur = longueur;
		this.marque = marque;
		this.modele = modele;
		this.remorque = remorque;
		this.type = type;
		this.typeprioritaire = typeprioritaire;
	}
	
	public void ajouteInformationDynamique(double cap, boolean clignotant, boolean freine, boolean gyrophare,
			double[] lonlat, Phare phare, double vitesseAbsolue) {
		InformationsDynamiques info = new InformationsDynamiques(cap, clignotant, freine, gyrophare, lonlat, phare, vitesseAbsolue);
		
		if(informationsDynamiques.isEmpty() || !informationsDynamiques.get(informationsDynamiques.size()-1).equals(info))
			informationsDynamiques.add(info);
	}
	
	
}
