package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Vérification des arguments (pour l'étape d'amélioration demandée par le PDF)
        // Si l'utilisateur ne donne pas de fichier, on prend une valeur par défaut
        String fileToRead = (args.length > 0) ? args[0] : "/user/root/input/achats.txt";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fileToRead);

        if (!fs.exists(path)) {
            System.out.println("Fichier introuvable : " + fileToRead);
            return;
        }

        // 1. Ouvrir le flux de données (Stream)
        FSDataInputStream inStream = fs.open(path);

        // 2. Envelopper le flux pour lire ligne par ligne
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);

        // 3. Boucle de lecture
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        // 4. Fermeture propre
        br.close();
        inStream.close();
        fs.close();
    }
}
