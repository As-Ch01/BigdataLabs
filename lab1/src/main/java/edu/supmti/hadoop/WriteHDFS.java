package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        // Arguments: 0 = chemin du fichier, 1 = texte à écrire
        if (args.length < 2) {
            System.out.println("Usage: WriteHDFS <chemin_fichier> <message>");
            return;
        }

        String destination = args[0];
        String message = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(destination);

        // 1. Vérifier si le fichier existe pour ne pas l'écraser (sécurité)
        if (!fs.exists(path)) {
            // 2. Créer le fichier
            FSDataOutputStream outStream = fs.create(path);

            // 3. Écrire dedans. writeUTF écrit en binaire spécifique Java.
            // Pour du texte lisible simple, on préfère souvent writeBytes + un retour à la
            // ligne
            outStream.writeUTF("Bonjour tout le monde ! \n");
            outStream.writeUTF(message);

            // 4. Fermer le flux pour sauvegarder
            outStream.close();
            System.out.println("Fichier créé avec succès !");
        } else {
            System.out.println("Le fichier existe déjà.");
        }

        fs.close();
    }
}
