#!/usr/bin/env python3
import pandas as pd
import itertools
import duckdb

# --- Kobza harvester modules ---
from modules.database import Database, load_config
from modules.logger import Logger

# --- Custom functions ---
from transliterate import translit
from jellyfish import soundex

def custom_cyrillic_function(entryname: str, lang: str) -> str:
    if not entryname or not lang:
        return ""
    lang_map = {"rus": "ru", "ukr": "uk", "bul": "bg"}
    lc = lang_map.get(lang.lower(), "ru")
    transliterated = translit(entryname, lc, reversed=True)
    code = soundex(transliterated)
    return entryname[0] + code[1:] if code and entryname else code or ""

def blocking_key_udf(entryname: str, lang: str) -> str:
    if not entryname:
        return ""
    c = entryname[0]
    return soundex(entryname) if c.isascii() and c.isalpha() else custom_cyrillic_function(entryname, lang)

# --- Logger & DB init ---
config = load_config()
config["logger"]["logfile"] = "log/authority_deduplication.log"
config["logger"]["name"] = "Splink_deduplication"
config["logger"]["console"] = True
logger = Logger(config["logger"])
db = Database(config["database"], logger)
db.connect()
logger.info("DB connected & logger ready.")

# --- Load normalized data ---
query = """
SELECT a_n.*, a.used_count
FROM authsource_normalized AS a_n
LEFT JOIN authsource AS a
  ON a_n.auth_id = a.auth_id
"""
df_norm = pd.read_sql(query, db.connection)
# Normalize column names to lowercase
df_norm.columns = df_norm.columns.str.lower()
logger.info(f"Loaded and normalized {len(df_norm)} records.")

# --- Preprocess ---
df_norm["year_of_birth"] = df_norm["dates"].str.extract(r'(\d{4})')[0]
df_norm["year_of_death"] = df_norm["dates"].str.extract(r'-(\d{4})')[0]
df_norm['proc_given_name'] = df_norm['given_name'].where(
    df_norm['given_name'].notna() & df_norm['given_name'].str.strip().astype(bool),
    df_norm['initials'].fillna("")
)
logger.info("Extracted text-based year fields and proc_given_name.")

# --- DuckDB UDFs on public connection ---
db_conn = duckdb.connect(':memory:')
for name, fn, params, ret in [
    ('blocking_key_udf', blocking_key_udf, ['VARCHAR','VARCHAR'], 'VARCHAR'),
    ('custom_cyrillic_function', custom_cyrillic_function, ['VARCHAR','VARCHAR'], 'VARCHAR'),
    ('soundex', soundex, ['VARCHAR'], 'VARCHAR'),
]:
    db_conn.create_function(name, fn, params, ret)
logger.info("Registered blocking UDFs.")

# --- Splink setup ---
from splink import DuckDBAPI, SettingsCreator, Linker, block_on
import splink.comparison_library as cl

# UDF-backed DuckDB API
db_api = DuckDBAPI(db_conn)

# Blocking rules
blocking_rules = [
    "blocking_key_udf(l.entryname, l.lang) = blocking_key_udf(r.entryname, r.lang)",
    "blocking_key_udf(l.entryname, l.lang) = blocking_key_udf(r.proc_given_name, r.lang)"
]

# Custom comparison for entryname & proc_given_name
comparison_entryname_givenname = {
   'output_column_name':'full_name',
   'comparison_levels':[
      {'sql_condition':'(entryname_l IS NULL AND proc_given_name_l IS NULL) AND (entryname_r IS NULL AND proc_given_name_r IS NULL)','label_for_charts':'Both null','is_null_level':True},
      {'sql_condition':'full_name_l = full_name_r','label_for_charts':'Exact full_name','tf_adjustment_column':'full_name','tf_adjustment_weight':1.0},
      {'sql_condition':'entryname_l = proc_given_name_r AND entryname_r = proc_given_name_l','label_for_charts':'Reversed match'},
      {'sql_condition':'jaro_winkler_similarity(entryname_l,entryname_r)>=0.92 AND jaro_winkler_similarity(proc_given_name_l,proc_given_name_r)>=0.92','label_for_charts':'Both Jaro>=0.92'},
      {'sql_condition':'jaro_winkler_similarity(entryname_l,entryname_r)>=0.88 AND jaro_winkler_similarity(proc_given_name_l,proc_given_name_r)>=0.88','label_for_charts':'Both Jaro>=0.88'},
      {'sql_condition':'ELSE','label_for_charts':'Else'}
   ],
   'comparison_description':'EntrynameGivennameComparison'
}

# Assemble comparisons
comparisons = [
    cl.ExactMatch('isni'),
    comparison_entryname_givenname,
    cl.ExactMatch('roman').configure(term_frequency_adjustments=True),
    cl.LevenshteinAtThresholds('year_of_birth',1),
    cl.LevenshteinAtThresholds('year_of_death',1),
]

# Splink settings
settings = SettingsCreator(
    link_type='dedupe_only', unique_id_column_name='id',
    probability_two_random_records_match=0.001,
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=comparisons,
    retain_intermediate_calculation_columns=True
)
linker = Linker(df_norm, settings, db_api=db_api)
logger.info("Splink initialized with updated rules and comparisons.")

# --- Training & Inference ---
det_rules = [block_on('entryname', 'lang'), 'l.isni = r.isni']
linker.training.estimate_probability_two_random_records_match(det_rules, recall=0.5)
# linker.training.estimate_parameters_using_expectation_maximisation(block_on('full_name'))
linker.training.estimate_parameters_using_expectation_maximisation(block_on('blocking_key_udf(entryname, lang)'))
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
df_pred = linker.inference.predict(threshold_match_probability=0.9)
linker.visualisations.comparison_viewer_dashboard(df_pred,'scv_authority_deduplication.html',overwrite=True)
logger.info("Dashboard saved.")

# --- Clustering ---
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(df_pred, threshold_match_probability=0.9)
df_clusters = clusters.as_pandas_dataframe()
df_clusters.columns = df_clusters.columns.str.lower()
linker.visualisations.cluster_studio_dashboard(
    df_pred, clusters, "cluster_studio.html", sampling_method="by_cluster_size", overwrite=True
)

# write out every cluster member
df_clusters.to_csv("auth_deduplication_clusters.csv", index=False)
logger.info("Wrote auth_deduplication_clusters.csv")

# prepare cluster → auth mappings
cluster_results = []
for _, row in df_clusters.iterrows():
    cluster_results.append({
        "auth_id":      int(row["auth_id"]),
        "cluster_id":   int(row["cluster_id"])
    })

# insert into your table
logger.info(f"Inserting {len(cluster_results)} rows into authsource_clusters…")
db.insert_many(
    "authsource_clusters",
    cluster_results,
    batch_size=10000
)
logger.info("Cluster assignments saved to authsource_clusters.")

# --- Helper: generate links ---
def generate_links(df_clusters, db):
    # load server → domain mapping
    servers = pd.read_sql('SELECT server_id, staffdomain FROM servers', db.connection)
    srv_map = dict(zip(servers.server_id, servers.staffdomain))
    logger.info("Servers mapping loaded.")

    # for each server, aggregate links across all clusters and languages, only field 200/400 rows
    for srv, srv_grp in df_clusters.groupby('server_id', sort=True):
        # filter to only authority record rows of interest
        srv_grp = srv_grp[srv_grp['field'].isin([200, 400])]
        if srv_grp.empty:
            continue
        logger.info(f"Building links for server {srv} ({len(srv_grp)} rows)")
        dom = srv_map.get(srv)
        if not dom:
            logger.error(f"No domain for server_id={srv}")
            continue

        merge_lines, detail_lines = [], []

        # iterate by cluster and language
        for (cid, lg), cluster in srv_grp.groupby(['cluster_id', 'lang'], sort=True):
            # skip trivial groups
            if len(cluster) < 2:
                continue
            # logger.info(f"  Cluster {cid}, lang {lg}, rows {len(cluster)}")

            # dedupe on source_authid, preferring field=200 over 400
            cluster = cluster.sort_values(['source_authid', 'field'])
            unique = cluster.drop_duplicates('source_authid', keep='first')
            if unique.shape[0] < 2:
                continue

            # pick main record: highest used_count, tie-break on lowest id
            max_uc = unique['used_count'].astype(int).max()
            tied = unique[unique['used_count'].astype(int) == max_uc]
            main = tied.sort_values('id').iloc[0]
            main_name = main['full_name']

            # link to every other unique record in this cluster/lang
            others = unique[unique['source_authid'] != main['source_authid']]
            for _, rec in others.iterrows():
                uid = rec['source_authid']
                # url + tab + main_name + tab + cluster_id
                url = f"https://{dom}/cgi-bin/koha/authorities/merge.pl?authid={main['source_authid']}&authid={uid}"
                merge_lines.append(f"{url}	{cid}	{main_name}	{rec['full_name']}")

        # write single file per server if any links
        if merge_lines or detail_lines:
            fname = f"links_server_{srv}.txt"
            with open(fname, 'w', encoding='utf-8') as f:
                if merge_lines:
                    f.write(
                        "Merge Links:\n" + "\n".join(sorted(set(merge_lines)))
                    )
                    
                if detail_lines:
                    f.write(
                        "\n\nDetail Links:\n" + "\n".join(sorted(set(detail_lines)))
                    )
            logger.info(f"Wrote {fname}")

# --- Run link generation ---
generate_links(df_clusters, db)
logger.info("Done.")