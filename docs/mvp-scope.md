# Périmètre du MVP Trendx

## Source
- Instance ThingsBoard : http://10.0.0.1:8081
- Device de test : CerboGx
- Device ID : f3ce3c10-9b9f-11f0-8762-cdf08b46b3be
- Clé de télémétrie : 
- Unité :
- Fréquence nominale :

## Données
- Historique à extraire : 90 jours
- Agrégation d’entrée : 1 heure
- Fuseau de stockage : UTC
- Politique pour les valeurs manquantes :
- Valeur minimale métier :
- Valeur maximale métier :

## Prévision
- Premier algorithme : Prophet
- Horizon : 24 heures
- Fréquence : 1 heure
- Réentraînement : quotidien
- Prévision : toutes les heures
- Clé de sortie : _EPD_<metric_name>

## Validation
- Métrique principale : MAE
- Métriques secondaires : RMSE, sMAPE
- Erreur maximale acceptable :
- Couverture minimale de l’intervalle de confiance :

## Environnement
- Premier environnement : développement
- Writeback autorisé uniquement sur le device de test
- Création d’alarmes désactivée pendant le MVP initial