import sys

current_word = None
current_count = 0

# Hadoop envoie les sorties du mapper TRIÉES par mot au reducer
for line in sys.stdin:
    line = line.strip()
    # On sépare le mot et le "1"
    word, count = line.split('\t', 1)
    count = int(count)

    # Si le mot est le même que le précédent, on augmente le compteur
    if current_word == word:
        current_count += count
    else:
        # Si c'est un nouveau mot, on affiche le total du mot précédent
        if current_word:
            print('%s\t%s' % (current_word, current_count))
        current_word = word
        current_count = count

# Ne pas oublier d'afficher le tout dernier mot
if current_word == word:
    print('%s\t%s' % (current_word, current_count))