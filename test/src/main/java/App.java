import giraudsa.marshall.exception.MarshallExeption;
import giraudsa.marshall.serialisation.text.json.JsonMarshaller;

{
    public static void main( String[] args ) throws InterruptedException, MarshallExeption
    {
        Vehicule voiture = new Vehicule(24, 1.8, "AB404DM",2.3, 5.2, "Honda", "Civic", false, TypeVehicule.Automobile, TypePrioritaire.NonPrioritaire);
        voiture.ajouteInformationDynamique(Math.PI/2, false, false, false, new double[]{5.88115900754929, 46.7561533343491}, Phare.Off, 36);
        Thread.sleep(100);
        voiture.ajouteInformationDynamique(Math.PI/2, false, false, false, new double[]{5.88110872372141, 46.7561533343491}, Phare.Off, 36);
        Thread.sleep(100);
        voiture.ajouteInformationDynamique(Math.PI/2, false, false, false, new double[]{5.88106185539883, 46.7561533343491}, Phare.Off, 36);
        Thread.sleep(100);
        voiture.ajouteInformationDynamique(Math.PI/2, false, false, false, new double[]{5.88101817058495, 46.7561533343491}, Phare.PF, 36);
        
        String json = JsonMarshaller.toCompleteJson(voiture);
    }
}
