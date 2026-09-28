# W100 — Historique d'exécutions durable

## Objet et périmètre

W99 définit l'historique ; **W100 lui donne une durability réelle**. W100
introduit `DurableExecutionStore` : un fichier JSON unique, écrit de manière
atomique, verrouillé entre processus, lisible après redémarrage.

W100 **ne change pas le contrat** de `ExecutionRecord` (W98) ni celui de
`ExecutionQuery` (W99). Il change uniquement le support de stockage.

Code source : `src/trendx/forecasting/execution.py` (section `DurableExecutionStore`).
Tests : `tests/unit/test_w100_durable_execution_history.py` (791 lignes,
commit `5968514`).

## Format sur disque

Un seul fichier JSON :

```json
{"format": 1, "records": {"<execution_id>": { ...ExecutionRecord... }}}
```

- `FORMAT_VERSION = 1`. Un `format` différent est **refusé** à la lecture
  (`ExecutionStoreSerializationError`), pas toléré ni converti.
- `records` est indexé par `execution_id`. Toute incohérence entre la clé et le
  champ `execution_id` du record est rejetée.
- Sérialisation canonique : `sort_keys=True`, `separators=(",", ":")`,
  `ensure_ascii=False`, `allow_nan=False`. Les valeurs non sérialisables en
  JSON strict sont refusées à l'écriture.
- Le fichier s'achève par un saut de ligne.

## Verrouillage

Deux niveaux, complémentaires :

| Niveau | Mécanisme | Portée |
|---|---|---|
| intra-processus | `threading.RLock` | threads du même processus |
| inter-processus | `fcntl.flock(fd, LOCK_EX)` puis `LOCK_UN` | processus distincts sur le même hôte |

Le fichier de verrouillage est un fichier séparé, créé en mode `a+`, dont le
mode est forcé à `0o600` avant tout `flock`. `LOCK_UN` est toujours relâché
dans un `finally`.

## Écriture atomique

La séquence est figée, et un crash à n'importe quel point ne peut pas publier un
fichier partiellement valide :

```text
tempfile.mkstemp(prefix=".<nom>.", suffix=".tmp", dir=<même répertoire>)
  -> os.fchmod(fd, 0o600)
  -> write + "\n"
  -> flush()
  -> os.fsync(fd)
  -> os.replace(tmp, chemin)      # atomique sur le même système de fichiers
```

Si une étape échoue, le fichier temporaire est `unlink`é dans un `finally` et
l'ancien fichier reste intact. Le temporaire est créé **dans le même
répertoire** que la cible, ce qui est indispensable : `os.replace` n'est
atomique qu'au sein d'un même système de fichiers.

## Permissions

| Objet | Mode |
|---|---|
| fichier temporaire | `0o600` |
| fichier de verrouillage | forcé à `0o600` |

## Sécurité du chemin

`DurableExecutionStore` refuse un chemin qui est un **symlink**. Le store d'audit
W110 et l'archive W112 appliquent la même règle.

## Reprise après redémarrage

Il n'y a **aucun** état en mémoire à reconstruire : le fichier EST l'état. Au
démarrage, la première lecture recharge et revalide l'intégralité des records.
Un fichier vide ou non-JSON est traité comme **corrompu**
(`ExecutionStoreSerializationError`), pas comme un store vide.

## `MemoryExecutionStore` vs `DurableExecutionStore`

| Critère | `MemoryExecutionStore` | `DurableExecutionStore` |
|---|---|---|
| Support | dictionnaire | fichier JSON |
| Version de format | — | `FORMAT_VERSION = 1` |
| Survit au redémarrage | non | oui |
| Multi-processus | non | oui (`flock`) |
| Atomicité d'écriture | sans objet | `fsync` + `os.replace` |
| Verrou inter-processus | non | oui |
| Usage | tests, DryRun | production |

## Idempotence

- `create()` refuse un `execution_id` **et** un `reference_key` déjà présents.
  Deux créations concurrentes de la même identité ne produisent pas deux
  enregistrements actifs.
- `mark_started`, `mark_success`, `mark_failed` sont des transitions d'état, pas
  des ajouts : les rejouer sur un état terminal est refusé.
- `delete_if_unchanged` (ajouté par W107) supprime **uniquement** si le record
  lu est identique à celui fourni. Un record modifié entre-temps n'est jamais
  supprimé.
- `restore_if_absent` (ajouté par W108) n'écrase jamais un enregistrement déjà
  présent.

## LIMITES

Ces limites sont **réelles et assumées** :

- **Hôte unique.** `flock` ne protège que le système de fichiers local. Deux
  hôtes distincts partageant le même stockage ne sont pas protégés.
- **Système de fichiers local uniquement.** Aucun NFS, aucun stockage objet,
  aucune réplication.
- **Pas de réplication ni de sauvegarde intégrée.** La sauvegarde du fichier
  d'historique est une responsabilité d'exploitation, hors de W100.
- **Coût d'écriture croissant.** Chaque mutation réécrit le fichier entier.
  `os.replace` est atomique, pas incrémental. À grande échelle, le coût est
  linéaire en la taille du store.
- **`flock` sous Windows.** `fcntl` n'existe pas sur Windows ; le module n'est
  pas portable.

Documentation voisine : [W98](w98-forecast-execution-persistence.md),
[W99](w99-execution-history.md), [W107](w107-execution-history-lifecycle.md),
[W108](w108-execution-history-recovery.md).
