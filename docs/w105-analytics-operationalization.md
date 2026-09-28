# W105 — Opérationnalisation de l'analytics

## Objet et périmètre

W105 ne livre **aucune fonctionnalité** : il valide l'analytics W104 et le
reporting W106 dans un contexte opérationnel réel (processus Uvicorn réel,
clients HTTP réels, données seedées dans un répertoire temporaire).

Le seul commit de la phase est `80298d7`
(`test(forecasting): validate execution history analytics operations`), qui
n'ajoute **que** :
`tests/integration/test_w105_execution_history_analytics_ops.py` (1028 lignes).

Il n'y a **aucun** module source W105. W105 est une phase de preuve.

## Ce que la phase établit

| Propriété | Moyen de preuve |
|---|---|
| l'analytics est stable sur processus réel | Uvicorn + httpx, plusieurs requêtes successives |
| l'analytics ne mute pas le store | empreinte SHA-256 du store avant / après |
| l'analytics respecte l'isolation de tenant | deux tenants seedés, aucun mélange dans les réponses |
| la pagination W99 est bien inerte sur l'analytics | pages de tailles différentes, agrégat identique |
| la sémantique temporelle est respectée | bornes inclusives, ordre ascendant, UTC |
| la lecture est sûre en concurrence | lectures HTTP parallèles, résultat identique |
| les erreurs sont assainies | aucun chemin, traceback ou SQL dans les réponses d'erreur |

## Observations de performance

**Ce sont des observations, pas un SLA.** Elles ne-valident pas un engagement.
Mesurées sur ce hôte, en processus, 3 répétitions, médiane, jeu de données
synthétique d'exécutions réelles.

| Enregistrements | `analyze()` médiane | `report()` médiane |
|---|---|---|
| 100 | 8,475 ms | 7,616 ms |
| 500 | 35,330 ms | 35,125 ms |
| 1000 | 72,658 ms | 70,319 ms |

Lecture : la croissance est **linéaire** en nombre d'enregistrements filtrés
(×10 enregistrements ≈ ×8,5 temps). L'analytics parcourt le périmètre filtré et
n'agrège pas en SQL.

**Aucune de ces valeurs n'est un seuil contractuel.** Il n'existe pas de SLA
pour l'analytics dans le dépôt. Si un jour un SLA est nécessaire, il doit être
défini et mesuré séparément — ce document ne le crée pas.

## LIMITES ET SUIVIS

- Les mesures ci-dessus sont **mono-processus, mono-hôte, sans concurrence**.
  Elles ne disent rien du comportement sous charge parallèle.
- La volumétrie testée va jusqu'à 1000 enregistrements. **Au-delà, le comportement
  n'est pas mesuré** et le coût linéaire suggère qu'il faut dimensionner en
  conséquence.
- W105 ne valide pas la **qualité** de l'analytics, seulement sa stabilité et
  son isolation. La justesse des agrégats est couverte par W104.

Documentation voisine : [W104](w104-execution-history-analytics.md),
[W106](w106-execution-history-reporting.md).
