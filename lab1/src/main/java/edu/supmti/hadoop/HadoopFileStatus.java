package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        // 1. Charger la configuration Hadoop (core-site.xml, etc.)
        Configuration conf = new Configuration();

        try {
            // 2. Se connecter au système de fichiers (HDFS)
            FileSystem fs = FileSystem.get(conf);

            // 3. Définir le chemin du fichier cible sur HDFS
            // Note: Le PDF utilise "/user/root/input/purchases.txt"
            Path filepath = new Path("/user/root/input/purchases.txt");

            // 4. Vérifier si le fichier existe
            if (!fs.exists(filepath)) {
                System.out.println("Le fichier n'existe pas !");
                System.exit(1); // Arrête le programme si pas de fichier
            }

            // 5. Récupérer les métadonnées (statut) du fichier
            FileStatus status = fs.getFileStatus(filepath);

            // 6. Afficher les informations
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("Replication Factor: " + status.getReplication());
            System.out.println("Block Size: " + status.getBlockSize());

            // 7. Informations sur les blocs (où sont stockés les morceaux du fichier ?)
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                // Affiche les machines (datanodes) qui possèdent ce bloc
                for (String host : blockLocation.getHosts()) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // 8. Renommer le fichier (de purchases.txt à achats.txt)
            Path newPath = new Path("/user/root/input/achats.txt");
            fs.rename(filepath, newPath);
            System.out.println("Fichier renommé avec succès en achats.txt");

            // Fermer la connexion
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}