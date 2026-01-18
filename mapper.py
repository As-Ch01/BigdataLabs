import sys

# sys.stdin reçoit le texte ligne par ligne envoyé par Hadoop
for line in sys.stdin:
    # On nettoie les espaces inutiles au début et à la fin
    line = line.strip()
    # On découpe la ligne en liste de mots
    words = line.split()
    # Pour chaque mot, on affiche le mot suivi d'un "1" séparé par une tabulation
    for word in words:
        print('%s\t%s' % (word, 1))