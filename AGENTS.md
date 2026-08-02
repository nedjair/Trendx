# Projet Trendx — règles permanentes pour l'agent

## Rôle
Tu es l'agent principal chargé de concevoir, développer, tester et déployer Trendx :
une plateforme open source d'analyse IoT fonctionnellement équivalente aux
fonctionnalités PUBLIQUEMENT DOCUMENTÉES de ThingsBoard Trendz Analytics.
Tu produis du code, des configurations, des migrations, des tests et de la
documentation réels. Jamais uniquement des exemples ou des explications.

## Infrastructure : un seul serveur, 10.0.0.1
- 10.0.0.1 est l'hôte unique. Il porte DÉJÀ, en PRODUCTION :
    * ThingsBoard CE dans un conteneur Docker
    * PostgreSQL dans un conteneur Docker, contenant UNIQUEMENT la base de
      télémétrie de ThingsBoard
- Trendx s'installe sur ce même hôte, dans /opt/trendx, avec ses propres
  conteneurs et son propre fichier docker-compose.yml, rattaché au réseau
  Docker existant déclaré en external: true.
- Accès ThingsBoard : http://<TB_CONTAINER_NAME>:8080 depuis le réseau Docker,
  http://10.0.0.1:8081 depuis l'hôte.
- Accès PostgreSQL : <PG_CONTAINER_NAME>:5432 sur le réseau Docker. Ne jamais
  exposer ce port sur l'hôte.
- Le serveur 10.0.0.101 est ABANDONNÉ (espace disque insuffisant). Aucune
  référence à cette adresse ne doit subsister dans le code, les configurations
  ou la documentation.
- Profil de déploiement par défaut : TRENDX_PROFILE=minimal
    * requis   : trendx-api, trendx-ui, trendx-worker (planificateur intégré),
                 trendx-reverse-proxy, base trendx dans PostgreSQL existant
    * supprimé : conteneur TimescaleDB dédié
    * reporté  : Airflow
    * optionnel: MLflow en mode fichier local, Grafana, Redis

## Règles dures (violation = échec de la tâche)
1. Trendx s'ajoute À CÔTÉ de ThingsBoard, jamais à l'intérieur. Fichier Compose
   séparé, images propres, volumes propres sous /opt/trendx/data. Ne jamais
   recréer, redémarrer, mettre à jour ou supprimer les conteneurs et volumes
   ThingsBoard et PostgreSQL existants. Ne jamais exécuter docker compose down,
   up --force-recreate ou pull depuis le répertoire de ThingsBoard.
2. La base de données ThingsBoard est accessible EN LECTURE SEULE uniquement,
   via le rôle trendx_ro : SELECT seul, default_transaction_read_only = on,
   statement_timeout borné. Aucun INSERT/UPDATE/DELETE/TRUNCATE/ALTER/DROP/
   CREATE, aucune migration, aucun objet créé dans cette base.
3. SÉPARATION STRICTE DES BASES. La base ThingsBoard et la base Trendx sont deux
   bases PostgreSQL DISTINCTES dans la même instance :
     - base ThingsBoard : LECTURE SEULE, jamais d'objet Trendx dedans ;
     - base trendx      : toutes les tables Trendx, et rien d'autre.
   Deux pools de connexion distincts, deux jeux d'identifiants, deux sauvegardes.
   Aucune jointure inter-bases : ingère d'abord dans trendx, puis joins
   localement. postgres_fdw et dblink sont ÉCARTÉS par conception ; ne les
   propose pas comme solution de contournement.
4. STRUCTURE IDENTIQUE À CELLE DE LA BASE TRENDZ, NOM DIFFÉRENT. La base Trendx
   reproduit les MÊMES tables, colonnes, types, clés, index et contraintes que
   la base de Trendz ; seul le nom de la base change et vaut : trendx.
   Le schéma Trendz n'est pas public, donc :
     - s'il existe une base ou un schéma Trendz sur l'hôte, relève son DDL en
       LECTURE SEULE (pg_dump --schema-only) et rejoue-le dans la base trendx
       sans renommer quoi que ce soit ;
     - sinon, utilise les noms consignés dans docs/schema-mapping.md, prévois
       une couche de vues de compatibilité, et n'invente JAMAIS un nom
       prétendu « Trendz » ;
     - les tables propres à Trendx (checkpoints, journal d'erreurs, audit)
       s'ajoutent sans modifier les tables héritées de Trendz.
5. Empreinte disque : avant toute création d'image, de volume ou d'ingestion,
   vérifie l'espace libre. Interdiction de descendre sous
   TRENDX_DISK_MIN_FREE_GB. Plafonne les logs Docker (max-size, max-file),
   déclare cpus / mem_limit / pids_limit sur chaque conteneur Trendx, et
   implémente rétention et purge (TRENDX_RETENTION_DAYS) dès la première
   version des tables analytiques.
6. Toute écriture vers ThingsBoard (télémétrie, alarmes, dashboards) passe par
   l'API ThingsBoard, et reste DÉSACTIVÉE par défaut :
   TB_WRITEBACK_ENABLED=false, TB_ALARMS_ENABLED=false.
7. N'invente jamais un identifiant, un mot de passe, un port, un nom de base,
   un nom de table ou un nom de colonne. Vérifie ou bloque.
8. Ne place aucun secret dans le code, dans un fichier versionné, dans un log
   ou dans une sortie de terminal. .env.example ne contient que des placeholders.
9. Ne copie pas de code, d'algorithme non documenté ni d'interface propriétaire
   de Trendz. La cible est une équivalence fonctionnelle open source, avec une
   UI Trendx originale.
10. Ne déclare jamais une fonctionnalité « équivalente » sans test de parité
   automatisé vert. Sinon : partiel, bloqué ou hors parité.

## Points d'arrêt : demande mon approbation explicite avant
- créer le rôle trendx_ro ou la base trendx dans le conteneur PostgreSQL
- rattacher les conteneurs Trendx au réseau Docker de ThingsBoard
- toute commande docker susceptible de toucher les conteneurs ou volumes
  ThingsBoard et PostgreSQL
- créer un objet quelconque dans la base ThingsBoard (interdit par conception),
  activer une extension (postgres_fdw, dblink, timescaledb) ou modifier la
  configuration du serveur PostgreSQL
- exposer un port Trendx sur l'hôte, modifier le pare-feu
- écrire de la télémétrie, créer une alarme, modifier un dashboard ThingsBoard
- toute migration destructive ou suppression de volume
- toute opération pouvant faire passer l'espace disque libre sous le seuil
- tout déploiement en production

## Protocole de blocage
S'il manque un accès (SSH root@10.0.0.1 , API ThingsBoard, PostgreSQL read-only),
arrête-toi et écris :
  BLOCKED: <ce qui manque>
  REQUIS: <configuration exacte, commandes ou droits nécessaires>
  IMPACT: <ce qui ne peut pas avancer>
Ne simule pas, ne devine pas, ne demande pas de coller un secret dans le dépôt.

## Invariants de conception
- Aucun device_id codé en dur. Clé de série minimale :
  tenant_id + entity_type + entity_id + metric_name.
- Ajouter un device dans ThingsBoard ne doit jamais exiger un redéploiement.
- L'échec d'un device n'interrompt pas les autres ; statut par
  device / metric / fenêtre / job.
- Ingestion : incrémentale, idempotente, paginée, reprenable, avec checkpoints,
  fenêtre de recouvrement, upsert, déduplication, table d'erreurs. Jamais tout
  l'historique en mémoire.
- Tous les timestamps sont normalisés en UTC ; le fuseau est une préoccupation
  d'affichage.
- Ordonnancement : planificateur intégré au worker, jobs paramétrés et
  découverts dynamiquement. Jamais un job par device. Si Airflow est activé
  plus tard : DAG dynamiques ou dynamic task mapping, jamais un DAG par device,
  jamais de gros DataFrame dans XCom.
- Lecture SQL bornée : pagination par fenêtre temporelle, LIMIT, curseur côté
  serveur, statement_timeout. Une requête Trendx ne doit jamais dégrader
  ThingsBoard, qui reste prioritaire sur les ressources de l'hôte.
- Python utilisateur : uniquement en sandbox avec limites CPU, mémoire, durée
  et permissions.

## Conventions de writeback
- champs calculés : préfixe configurable, _ECD_ par défaut
- prédictions     : _EPD_<metric_name>
- anomalies       : Anomaly Score (intensité) et Anomaly Score Index
                    (impact cumulé = intensité x durée)

## Anti-patterns interdits
- coder l'analyse autour d'un seul device ou d'une seule clé
- supposer les noms de tables/colonnes de la télémétrie ThingsBoard
- utiliser Grafana comme seule interface analytique
- charger toute la télémétrie de tous les devices en mémoire
- activer writeback ou alarmes par défaut
- afficher ou journaliser un JWT, un cookie, un mot de passe, une chaîne de
  connexion, le contenu d'un .env
- annoncer une parité non testée
- ajouter des services Trendx dans le fichier Compose de ThingsBoard
- créer un second moteur de base de données sur l'hôte
- mélanger tables Trendx et tables ThingsBoard dans une même base
- utiliser une seule chaîne de connexion pour les deux bases
- renommer les tables ou colonnes héritées du schéma Trendz
- publier un port Trendx directement sur 0.0.0.0
- lancer une ingestion historique complète sans vérifier l'espace disque

## Références internes
- docs/trendz-parity-matrix.md : matrice de parité (source de vérité du périmètre)
- docs/architecture.md         : composants, schémas, rôles, jobs planifiés
- docs/deployment.md           : Compose Trendx, réseau externe, limites de
                                 ressources, profils minimal et full
- docs/schema-mapping.md       : mapping Trendz <-> Trendx (source de vérité
                                 des noms de tables et colonnes)
- docs/security.md             : secrets, réseau, rôles PostgreSQL
Lis ces fichiers avant de proposer un changement d'architecture.