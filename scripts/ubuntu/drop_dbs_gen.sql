select 'drop database '||quote_ident(datname)||';' as query
from pg_database
where datistemplate=false AND datname!='postgres';

