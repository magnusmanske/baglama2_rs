#!/bin/bash
#ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@login.toolforge.org -L 3377:commonswiki.web.db.svc.eqiad.wmflabs:3306 -N &
# Commons links tables (x4 cluster), split off September 2026
ssh magnus@login.toolforge.org -L 3378:links.commonswiki.web.db.svc.wikimedia.cloud:3306 -N &
#ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
#ssh magnus@login.toolforge.org -L 3317:termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud:3306 -N &
ssh magnus@login.toolforge.org -L 3333:oxztsldqokc.svc.trove.eqiad1.wikimedia.cloud:3306 -N &
