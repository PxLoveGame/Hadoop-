package Tri;


// =========================================================================
// COMPARATEURS
// =========================================================================

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparateur qui inverse la méthode de comparaison d'un sous-type T de WritableComparable (ie. une clé).
 */
@SuppressWarnings("rawtypes")

public class InverseComparator<T extends WritableComparable> extends WritableComparator {

    public InverseComparator(Class<T> parameterClass) {
        super(parameterClass, true);
    }

    /**
     * Cette fonction définit l'ordre de comparaison entre 2 objets de type T.
     * Dans notre cas nous voulons simplement inverser la valeur de retour de la méthode T.compareTo.
     *
     * @return 0 si a = b <br>
     *         x > 0 si a > b <br>
     *         x < 0 sinon
     */
    @SuppressWarnings("unchecked")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        // On inverse le retour de la méthode de comparaison du type
        return -a.compareTo(b);
    }
}

/**
 * Inverseur de la comparaison du type Text.
 */
class TextInverseComparator extends InverseComparator<Text> {

    public TextInverseComparator() {
        super(Text.class);
    }
}

