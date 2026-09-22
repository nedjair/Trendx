"""Trendx scheduler handlers package (résolution W49/master, périmètre minimal).

Seul ``trendx.scheduler.handlers`` (source de vérité des handlers P0 +
``JOB_HANDLERS``) est présent : c'est tout ce que le worker et
l'orchestrateur ML consomment. Le reste du package B1 (registry, runtime,
leader, persistence, fanout, service) relève de l'activation du scheduler
dédié, gate séparée : il sera réintroduit depuis master à ce moment-là
(l'import ci-dessous reste volontairement absent pour ne pas dépendre
de ``trendx.database.repositories.SchedulerRunRepository``, qui n'existe
pas encore sur cette base).
"""
